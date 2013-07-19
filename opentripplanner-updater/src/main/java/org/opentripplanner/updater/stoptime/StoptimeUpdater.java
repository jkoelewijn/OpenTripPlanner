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
import org.opentripplanner.routing.edgetype.factory.GTFSPatternHopFactory;
import org.opentripplanner.routing.graph.Edge;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.routing.graph.Vertex;
import org.opentripplanner.routing.services.GraphService;
import org.opentripplanner.routing.services.TransitIndexService;
import org.opentripplanner.routing.trippattern.TripUpdate;
import org.opentripplanner.routing.trippattern.Update;
import org.opentripplanner.routing.services.TransitIndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Update OTP stop time tables from some (realtime) source
 * @author abyrd
 */
public class StoptimeUpdater implements Runnable, TimetableSnapshotSource {

    private static final Logger LOG = LoggerFactory.getLogger(StoptimeUpdater.class);

    @Setter
    @Autowired private GraphService graphService;
    @Setter    private UpdateStreamer updateStreamer;
    @Setter    private static int logFrequency = 2000;
    
    private int appliedBlockCount = 0;

    /**
     * Factory used for adding ADDED/UNSCHEDULED trips to the graph.
     */
    protected GTFSPatternHopFactory hopFactory = new GTFSPatternHopFactory();
    
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

    /** Should expired realtime data be purged from the graph. */
    @Setter private boolean purgeExpiredData = true;
    
    /** The TransitIndexService */
    private TransitIndexService transitIndexService;
    
    /** A map from ADDED Trip AgencyAndIds to the TripPatterns that contain them */
    // TODO: index on tripId + serviceId to be accurate...
    protected Map<AgencyAndId, GTFSPatternHopFactory.Result> addedPatternIndex = new HashMap<AgencyAndId, GTFSPatternHopFactory.Result>();
    
    protected Map<TableTripPattern, Set<AgencyAndId>> addedPatternUsageIndex = new HashMap<TableTripPattern, Set<AgencyAndId>>();
    
    protected Map<ServiceDate, Set<TableTripPattern>> addedTripPatternsByServiceDate = new HashMap<ServiceDate, Set<TableTripPattern>>();
    
    protected ServiceDate lastPurgeDate = null;
    
    // nothing in the timetable snapshot binds it to one graph. we could use this updater for all
    // graphs at once
    protected Graph graph;
    protected long lastSnapshotTime = -1;
    
    /**
     * Set the data sources for the target graphs.
     */
    @PostConstruct
    public void setup () {
        graph = graphService.getGraph();
        graph.timetableSnapshotSource = this;
        transitIndexService = graph.getService(TransitIndexService.class);
        if (transitIndexService == null)
            throw new RuntimeException(
                    "Real-time update need a TransitIndexService. Please setup one during graph building.");
    }
    
    public TimetableResolver getSnapshot() {
        return getSnapshot(false);
    }
    
    protected synchronized TimetableResolver getSnapshot(boolean force) {
        long now = System.currentTimeMillis();
        if (force || now - lastSnapshotTime > maxSnapshotFrequency) {
            if (force || buffer.isDirty()) {
                LOG.debug("Committing {}", buffer.toString());
                snapshot = buffer.commit(force);
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
     * and applies those updates to the graph.
     */
    @Override
    public void run() {
        graph = graphService.getGraph();
        transitIndexService = graph.getService(TransitIndexService.class);
        
        List<TripUpdate> tripUpdates = updateStreamer.getUpdates(); 
        if (tripUpdates == null) {
            LOG.debug("updates is null");
            return;
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
                if(addedPatternIndex.containsKey(tripUpdate.getTripId())) {
                    applied = handleRemovedTrip(tripUpdate);
                } else {
                    applied = handleCanceledTrip(tripUpdate);
                }
                break;
            case MODIFIED:
                applied = handleModifiedTrip(tripUpdate);
                break;
            case REMOVED:
                applied = handleRemovedTrip(tripUpdate);
                break;
            }
            
            if(!applied) {
                LOG.warn("Failed to apply trip update:\n" +  tripUpdate);
            } else {
                appliedBlockCount++;
            }
            
            if(appliedBlockCount % logFrequency == 0) {
                LOG.info("Appplied {0} trip updates.", appliedBlockCount);
            }
        }
        LOG.debug("end of update message");
        
        if(purgeExpiredData) {
            if(purgeExpiredData()) {
                getSnapshot(true);
            }
        }
    }

    protected boolean handleAddedTrip(TripUpdate tripUpdate) {
        ServiceDate serviceDate = tripUpdate.getServiceDate();
        AgencyAndId tripId = tripUpdate.getTripId();
        Trip trip = tripUpdate.getTrip();

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
        GTFSPatternHopFactory.Result result = hopFactory.addPatternForTripToGraph(graph, trip, stopTimes);
        TableTripPattern tripPattern = result.tripPattern;
        hopFactory.augmentServiceIdToNumberService(serviceIdToNumberService);
        addedPatternIndex.put(tripId, result);
        
        // TODO: for trips reusing a TripPattern this create two RouteVariants
        transitIndexService.add(result);

        Set<AgencyAndId> patternUsage = addedPatternUsageIndex.get(tripPattern);
        if(patternUsage == null) {
            patternUsage = new HashSet<AgencyAndId>();
        }
        patternUsage.add(tripId);
        addedPatternUsageIndex.put(tripPattern, patternUsage);

        Set<TableTripPattern> patternsForServiceDate = addedTripPatternsByServiceDate.get(serviceDate);
        if(patternsForServiceDate == null) {
            patternsForServiceDate = new HashSet<TableTripPattern>();
        }
        patternsForServiceDate.add(tripPattern);
        addedTripPatternsByServiceDate.put(serviceDate, patternsForServiceDate);
        
        tripPattern.setTraversable(true);

        return true;
    }

    protected boolean handleCanceledTrip(TripUpdate tripUpdate) {

        TableTripPattern pattern = transitIndexService.getPatternForTrip(tripUpdate.getTripId());
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
        TableTripPattern pattern = transitIndexService.getPatternForTrip(tripUpdate.getTripId());
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
        Graph graph = graphService.getGraph();

        AgencyAndId tripId = tripUpdate.getTripId();
        GTFSPatternHopFactory.Result result = addedPatternIndex.get(tripId); 
        TableTripPattern pattern = result.tripPattern;
        if(pattern == null) {
            LOG.warn("Attempt to remove non-added/non-existent pattern for " + tripUpdate);
            return false;
        }
        addedPatternIndex.remove(tripId);
        
        Set<AgencyAndId> patternUsage = addedPatternUsageIndex.get(pattern);
        if(patternUsage == null) {
            return true;
        }
        
        addedPatternUsageIndex.put(pattern, patternUsage);
        patternUsage.remove(tripId);
        
        // TODO: the trip is left in the TripPattern's timetable
        if(!patternUsage.isEmpty()) {
            return false;
        }

        pattern.setTraversable(false);
        
        addedPatternUsageIndex.remove(pattern);
        
        transitIndexService.remove(result);
        
        for(Edge e : result.edges) {
            e.detach();
        }

        for(Vertex v : result.vertices) {
            graph.remove(v);
        }
        
        ServiceDate serviceDate = tripUpdate.getServiceDate();
        Set<TableTripPattern> patternsForServiceDate = addedTripPatternsByServiceDate.get(serviceDate);
        if(patternsForServiceDate != null) {
            patternsForServiceDate.remove(pattern);
            addedTripPatternsByServiceDate.put(serviceDate, patternsForServiceDate);
        }
        
        return true;
    }
    
    protected boolean addService(AgencyAndId serviceId, TimeZone timezone, List<ServiceDate> days) {
        Graph graph = graphService.getGraph();
        CalendarServiceData data = graph.getService(CalendarServiceData.class);
        
        if(data.getServiceIds().contains(serviceId)) {
            return false;
        }
        
        data.putServiceDatesForServiceId(serviceId, days);

        LocalizedServiceId localizedServiceId = new LocalizedServiceId(serviceId, timezone);
        List<Date> dates = new LinkedList<Date>();
        for(ServiceDate day : days) {
            dates.add(day.getAsDate(timezone));
        }
        data.putDatesForLocalizedServiceId(localizedServiceId, dates);
        
        return true;
    }

    protected boolean purgeExpiredData() {
        ServiceDate today = new ServiceDate();
        ServiceDate previously = today.previous().previous(); // Just to be safe... 
        
        if(lastPurgeDate != null && lastPurgeDate.compareTo(previously) > 0) {
            return false;
        }
        
        LOG.debug("purging expired realtime data");
        
        boolean removed = false;
        if(addedTripPatternsByServiceDate.containsKey(previously)) {
            for(TableTripPattern tripPattern : addedTripPatternsByServiceDate.get(previously)) {
                for(Trip trip : tripPattern.getTrips()) {
                    TripUpdate removal = TripUpdate.forRemovedTrip(trip.getId(), new Date().getTime() / 1000, previously);
                    handleRemovedTrip(removal);
                }
            }
            
            addedTripPatternsByServiceDate.remove(previously);
            removed = true;
        }
        
        lastPurgeDate = previously;
        
        return buffer.purgeExpiredData(previously) || removed;
    }

    public String toString() {
        String s = (updateStreamer == null) ? "NONE" : updateStreamer.toString();
        return "StoptimeUpdater(streamer=" + s + ")";
    }
}
