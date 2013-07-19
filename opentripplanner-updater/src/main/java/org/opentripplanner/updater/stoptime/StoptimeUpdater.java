/* This program is free software: you can redistribute it and/or
 modify it under the terms of the GNU Lesser General Public License
 as published by the Free Software Foundation, either version 3 of
 the License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package org.opentripplanner.updater.stoptime;

import static org.opentripplanner.common.IterableLibrary.filter;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import javax.annotation.PostConstruct;

import lombok.Setter;

import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.Stop;
import org.onebusaway.gtfs.model.StopTime;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.model.calendar.CalendarServiceData;
import org.onebusaway.gtfs.model.calendar.LocalizedServiceId;
import org.onebusaway.gtfs.model.calendar.ServiceDate;
import org.onebusaway.gtfs.services.calendar.CalendarService;
import org.opentripplanner.routing.core.ServiceIdToNumberService;
import org.opentripplanner.routing.edgetype.TableTripPattern;
import org.opentripplanner.routing.edgetype.TimetableResolver;
import org.opentripplanner.routing.edgetype.TimetableSnapshotSource;
import org.opentripplanner.routing.edgetype.TransitBoardAlight;
import org.opentripplanner.routing.edgetype.factory.GTFSPatternHopFactory;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.routing.services.GraphService;
import org.opentripplanner.routing.services.TransitIndexService;
import org.opentripplanner.routing.trippattern.TripUpdate;
import org.opentripplanner.routing.trippattern.Update;
import org.opentripplanner.routing.vertextype.TransitStopDepart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Update OTP stop time tables from some (realtime) source
 * @author abyrd
 */
public class StoptimeUpdater implements Runnable, TimetableSnapshotSource {

    private static final Logger LOG = LoggerFactory.getLogger(StoptimeUpdater.class);

    @Autowired private GraphService graphService;
    @Setter    private UpdateStreamer updateStreamer;
    @Setter    private static int logFrequency = 2000;

    /**
     * Factory used for adding ADDED/UNSCHEDULED trips to the graph.
     */
    private GTFSPatternHopFactory hopFactory = new GTFSPatternHopFactory();
    
    /** 
     * If a timetable snapshot is requested less than this number of milliseconds after the previous 
     * snapshot, just return the same one. Thottles the potentially resource-consuming task of 
     * duplicating a TripPattern -> Timetable map and indexing the new Timetables.
     */
    @Setter private int maxSnapshotFrequency = 1000; // msec    

    /** 
     * The last committed snapshot that was handed off to a routing thread. This snapshot may be
     * given to more than one routing thread if the maximum snapshot frequency is exceeded. 
     */
    private TimetableResolver snapshot = null;
    
    /** The working copy of the timetable resolver. Should not be visible to routing threads. */
    private TimetableResolver buffer = new TimetableResolver();
    
    /** A map from Trip AgencyAndIds to the TripPatterns that contain them */
    private Map<AgencyAndId, TableTripPattern> patternIndex;
    
    /** A map from ADDED Trip AgencyAndIds to the TripPatterns that contain them */
    private Map<AgencyAndId, TableTripPattern> addedPatternIndex = new HashMap<AgencyAndId, TableTripPattern>();

    private Map<TableTripPattern, Set<AgencyAndId>> addedPatternUsageIndex = new HashMap<TableTripPattern, Set<AgencyAndId>>();
    
    // nothing in the timetable snapshot binds it to one graph. we could use this updater for all
    // graphs at once
    private Graph graph;
    private long lastSnapshotTime = -1;
    
    /**
     * Once the data sources and target graphs have been set, index all trip patterns on the 
     * tripIds of Trips they contain.
     */
    @PostConstruct
    public void setup () {
        graph = graphService.getGraph();
        patternIndex = new HashMap<AgencyAndId, TableTripPattern>();
        for (TransitStopDepart tsd : filter(graph.getVertices(), TransitStopDepart.class)) {
            for (TransitBoardAlight tba : filter(tsd.getOutgoing(), TransitBoardAlight.class)) {
                if (!tba.isBoarding())
                    continue;
                TableTripPattern pattern = tba.getPattern();
                for (Trip trip : pattern.getTrips()) {
                    patternIndex.put(trip.getId(), pattern);
                }
            }
        }
        graph.timetableSnapshotSource = this;
    }
    
    public synchronized TimetableResolver getSnapshot() {
        long now = System.currentTimeMillis();
        if (now - lastSnapshotTime > maxSnapshotFrequency) {
            if (buffer.isDirty()) {
                LOG.debug("Committing {}", buffer.toString());
                snapshot = buffer.commit();
            } else {
                LOG.debug("Buffer was unchanged, keeping old snapshot.");
            }
            lastSnapshotTime = System.currentTimeMillis();
        } else {
            LOG.debug("Snapshot frequency exceeded. Reusing snapshot {}", snapshot);
        }
        return snapshot;
    }
    
    /**
     * Repeatedly makes blocking calls to an UpdateStreamer to retrieve new stop time updates,
     * and applies those updates to scheduled trips.
     */
    @Override
    public void run() {
        int appliedBlockCount = 0;
        while (true) {
            List<TripUpdate> tripUpdates = updateStreamer.getUpdates(); 
            if (tripUpdates == null) {
                LOG.debug("updates is null");
                continue;
            }

            LOG.debug("message contains {} trip update blocks", tripUpdates.size());
            int uIndex = 0;
            for (TripUpdate tripUpdate : tripUpdates) {
                uIndex += 1;
                LOG.debug("update block #{} ({} updates) :", uIndex, tripUpdate.getUpdates().size());
                LOG.trace("{}", tripUpdate.toString());
                
                boolean applied = false;
                switch(tripUpdate.getStatus()) {
                case ADDED:
                    if(addedPatternIndex.containsKey(tripUpdate.getTripId())) {
                        applied = handleModifiedTrip(tripUpdate);
                    } else {
                        applied = handleAddedTrip(tripUpdate);
                    }
                    break;
                case CANCELED:
                    applied = handleCanceledTrip(tripUpdate);
                    break;
                case MODIFIED:
                    applied = handleModifiedTrip(tripUpdate);
                    break;
                case REMOVED:
                    applied = handleRemovedTrip(tripUpdate);
                    break;
                }
                
                if(applied) {
                   appliedBlockCount++;
                } else {
                    LOG.warn("Failed to apply Tripupdate: " + tripUpdate);
                }

                // TODO: logging...
            }
            LOG.debug("end of update message");
        }
    }

    protected boolean handleAddedTrip(TripUpdate tripUpdate) {
        ServiceDate serviceDate = tripUpdate.getServiceDate();
        AgencyAndId tripId = tripUpdate.getTripId();
        Trip trip = tripUpdate.getTrip();

        Graph graph = graphService.getGraph();
        TransitIndexService transitIndexService = graph.getService(TransitIndexService.class);;
        ServiceIdToNumberService serviceIdToNumberService = graph.getService(ServiceIdToNumberService.class);
        CalendarService calendarService = graph.getCalendarService();
        TimeZone timezone = TimeZone.getTimeZone(transitIndexService.getAgency(tripId.getAgencyId()).getTimezone());

        AgencyAndId serviceId = trip.getServiceId();
        if(serviceId == null) {
            serviceId = new AgencyAndId(trip.getId().getAgencyId(), "GTFS-RT-SERVICE-" + serviceDate);
            trip.setServiceId(serviceId);
        }
        if(!calendarService.getServiceIds().contains(serviceId)) {
            addService(serviceId, timezone, Collections.singletonList(serviceDate));
        }
        
        List<StopTime> stopTimes = new LinkedList<StopTime>();
        for(Update update : tripUpdate.getUpdates()) {
            Stop stop = transitIndexService.getAllStops().get(update.stopId);

            StopTime stopTime = new StopTime();
            stopTime.setTrip(trip);
            stopTime.setStop(stop);
            stopTime.setStopSequence(update.stopSeq);
            stopTime.setArrivalTime(update.arrive);
            stopTime.setDepartureTime(update.depart);

            stopTimes.add(stopTime);
        }

        hopFactory.bootstrapContextFromTransitIndex(transitIndexService, calendarService, serviceIdToNumberService);
        TableTripPattern tripPattern = hopFactory.addPatternForTripToGraph(graph, trip, stopTimes);
        hopFactory.augmentServiceIdToNumberService(serviceIdToNumberService);
        addedPatternIndex.put(tripId, tripPattern);
        
        Set<AgencyAndId> patternUsage = addedPatternUsageIndex.get(tripPattern);
        if(patternUsage == null) {
            patternUsage = new HashSet<AgencyAndId>();
        }
        patternUsage.add(tripId);
        addedPatternUsageIndex.put(tripPattern, patternUsage);

        tripPattern.setTraversable(true);

        return true;
    }

    protected boolean handleCanceledTrip(TripUpdate tripUpdate) {

        TableTripPattern pattern = getPatternForTrip(tripUpdate.getTripId());
        if (pattern == null) {
            LOG.debug("No pattern found for tripId {}, skipping UpdateBlock.", tripUpdate.getTripId());
            return false;
        }

        boolean applied = buffer.update(pattern, tripUpdate);
        if (applied) {
            // consider making a snapshot immediately in anticipation of incoming requests 
            getSnapshot(); 
        }
        
        return applied;
    }

    protected boolean handleModifiedTrip(TripUpdate tripUpdate) {

        tripUpdate.filter(true, true, true);
        if (! tripUpdate.isCoherent()) {
            LOG.warn("Incoherent UpdateBlock, skipping.");
            return false;
        }
        if (tripUpdate.getUpdates().size() < 1) {
            LOG.debug("UpdateBlock contains no updates after filtering, skipping.");
            return false;
        }
        TableTripPattern pattern = getPatternForTrip(tripUpdate.getTripId());
        if (pattern == null) {
            LOG.debug("No pattern found for tripId {}, skipping UpdateBlock.", tripUpdate.getTripId());
            return false;
        }

        // we have a message we actually want to apply
        boolean applied = buffer.update(pattern, tripUpdate);
        if (applied) {
            // consider making a snapshot immediately in anticipation of incoming requests 
            getSnapshot(); 
        }
        
        return applied;
    }

    protected boolean handleRemovedTrip(TripUpdate tripUpdate) {
        AgencyAndId tripId = tripUpdate.getTripId();
        TableTripPattern pattern = addedPatternIndex.get(tripId);
        if(pattern == null) {
            LOG.warn("Attempt to remove non-added/non-existent pattern for " + tripUpdate);
            return false;
        }
        
        Set<AgencyAndId> patternUsage = addedPatternUsageIndex.get(pattern);
        if(patternUsage == null) {
            return true;
        }
        
        patternUsage.remove(tripId);
        addedPatternUsageIndex.put(pattern, patternUsage);
        
        if(!patternUsage.isEmpty()) {
            return false;
        }
        
        pattern.setTraversable(false);
        
        //TODO: remove edges from graph
        
        return false;
    }
    
    protected TableTripPattern getPatternForTrip(AgencyAndId tripId) {
        TableTripPattern pattern = patternIndex.get(tripId);
        if(pattern == null) {
            pattern = addedPatternIndex.get(tripId);
        }
        return pattern;
    }
    
    public void addService(AgencyAndId serviceId, TimeZone timezone, List<ServiceDate> days) {
        Graph graph = graphService.getGraph();
        CalendarServiceData data = graph.getService(CalendarServiceData.class);
        data.putServiceDatesForServiceId(serviceId, days);

        LocalizedServiceId localizedServiceId = new LocalizedServiceId(serviceId, timezone);
        List<Date> dates = new LinkedList<Date>();
        for(ServiceDate day : days) {
            dates.add(day.getAsDate(timezone));
        }
        data.putDatesForLocalizedServiceId(localizedServiceId, dates);
    }

    public String toString() {
        String s = (updateStreamer == null) ? "NONE" : updateStreamer.toString();
        return "Streaming stoptime updater with update streamer = " + s;
    }
    
}
