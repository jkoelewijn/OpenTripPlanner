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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.junit.Before;
import org.junit.Test;
import org.onebusaway.gtfs.model.Agency;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.Route;
import org.onebusaway.gtfs.model.ServiceCalendar;
import org.onebusaway.gtfs.model.ServiceCalendarDate;
import org.onebusaway.gtfs.model.Stop;
import org.onebusaway.gtfs.model.calendar.ServiceDate;
import org.opentripplanner.routing.core.TraverseMode;
import org.opentripplanner.routing.edgetype.PreAlightEdge;
import org.opentripplanner.routing.edgetype.PreBoardEdge;
import org.opentripplanner.routing.edgetype.TableTripPattern;
import org.opentripplanner.routing.edgetype.factory.GTFSPatternHopFactory.Result;
import org.opentripplanner.routing.services.TransitIndexService;
import org.opentripplanner.routing.transit_index.RouteVariant;
import org.opentripplanner.routing.trippattern.TripTimes;
import org.opentripplanner.routing.trippattern.TripUpdate;
import org.opentripplanner.routing.trippattern.Update;

import com.google.transit.realtime.GtfsRealtime;
import com.vividsolutions.jts.geom.Coordinate;

public class GtfsRealtimeAbstractUpdateStreamerTest {
    private GtfsRealtimeAbstractUpdateStreamer streamer = null;
    private GtfsRealtime.FeedMessage.Builder feedMessage = null;
    
    private String agencyId = "A";
    private AgencyAndId tripId = new AgencyAndId(agencyId, "A");
    private AgencyAndId stopId = new AgencyAndId(agencyId, "B");
    private String routeId = "C";
    private ServiceDate serviceDate = new ServiceDate(2013, 12, 12);
    private String startDate = "20131212";
    private long today = serviceDate.getAsDate(TimeZone.getTimeZone("GMT")).getTime() / 1000;
    private long timestamp = 1;
    
    
    @Test
    public void testUpdateRouting() {
        List<TripUpdate> tripUpdates;
        TripUpdate tripUpdate;
        
        feedMessage = GtfsRealtime.FeedMessage.newBuilder();
        
        // bad start date
        addFeedHeader(feedMessage, 1);
        feedMessage.clearEntity();
        GtfsRealtime.FeedEntity.Builder entity = createEntity("R1");
        GtfsRealtime.TripUpdate.Builder rtTripUpdate = createTripUpdate(tripId.getId(), "Y" + startDate, GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
        entity.setTripUpdate(rtTripUpdate);
        feedMessage.addEntity(entity);
        
        tripUpdates = streamer.getUpdates();
        assertEquals(0, tripUpdates.size());
        
        // cancel trip
        addFeedHeader(feedMessage, 2);
        feedMessage.clearEntity();
        entity = createEntity("R2");
        rtTripUpdate = createTripUpdate(tripId.getId(), startDate, GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
        entity.setTripUpdate(rtTripUpdate);
        feedMessage.addEntity(entity);
        
        tripUpdates = streamer.getUpdates();
        assertEquals(1, tripUpdates.size());
        tripUpdate = tripUpdates.get(0);
        assertEquals(TripUpdate.Status.CANCELED, tripUpdate.getStatus());
        
        // added trip
        addFeedHeader(feedMessage, 3);
        feedMessage.clearEntity();
        entity = createEntity("R3");
        rtTripUpdate = createTripUpdate(tripId.getId(), startDate, routeId, GtfsRealtime.TripDescriptor.ScheduleRelationship.ADDED);
        addStopTimeUpdate(rtTripUpdate, stopId.getId(), today, 0, null);
        addStopTimeUpdate(rtTripUpdate, stopId.getId(), today, 1, null);
        entity.setTripUpdate(rtTripUpdate);
        feedMessage.addEntity(entity);
        
        tripUpdates = streamer.getUpdates();
        assertEquals(1, tripUpdates.size());
        tripUpdate = tripUpdates.get(0);
        assertEquals(TripUpdate.Status.ADDED, tripUpdate.getStatus());
        
        // unscheduled trip
        addFeedHeader(feedMessage, 4);
        feedMessage.clearEntity();
        entity = createEntity("R4");
        rtTripUpdate = createTripUpdate(tripId.getId(), startDate, routeId, GtfsRealtime.TripDescriptor.ScheduleRelationship.UNSCHEDULED);
        addStopTimeUpdate(rtTripUpdate, stopId.getId(), today, 0, null);
        addStopTimeUpdate(rtTripUpdate, stopId.getId(), today, 1, null);
        entity.setTripUpdate(rtTripUpdate);
        feedMessage.addEntity(entity);
        
        tripUpdates = streamer.getUpdates();
        assertEquals(0, tripUpdates.size());
        
        // replacement trip
        addFeedHeader(feedMessage, 5);
        feedMessage.clearEntity();
        entity = createEntity("R5");
        rtTripUpdate = createTripUpdate(tripId.getId(), startDate, GtfsRealtime.TripDescriptor.ScheduleRelationship.REPLACEMENT);
        addStopTimeUpdate(rtTripUpdate, stopId.getId(), today, 0, null);
        addStopTimeUpdate(rtTripUpdate, stopId.getId(), today, 1, null);
        entity.setTripUpdate(rtTripUpdate);
        feedMessage.addEntity(entity);
        
        tripUpdates = streamer.getUpdates();
        assertEquals(0, tripUpdates.size());
    }

    @Test
    public void testUpdateFeedTimestampFiltering() {
        List<TripUpdate> tripUpdates;
        
        GtfsRealtime.FeedEntity.Builder entity;
        GtfsRealtime.TripUpdate.Builder rtTripUpdate;
        
        feedMessage = null;
        assertNull(streamer.getUpdates());

        feedMessage = GtfsRealtime.FeedMessage.newBuilder();
        
        { // filter on feed timestamp
            addFeedHeader(feedMessage, 0);
            feedMessage.clearEntity();
            entity = createEntity("FT1");
            rtTripUpdate = createTripUpdate(tripId.getId(), startDate, GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
            entity.setTripUpdate(rtTripUpdate);
            feedMessage.addEntity(entity);
            
            tripUpdates = streamer.getUpdates();
            assertEquals(1, tripUpdates.size());
            
            addFeedHeader(feedMessage, 10);
            feedMessage.clearEntity();
            entity = createEntity("FT1");
            rtTripUpdate = createTripUpdate(tripId.getId(), startDate, GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
            entity.setTripUpdate(rtTripUpdate);
            feedMessage.addEntity(entity);
            
            tripUpdates = streamer.getUpdates();
            assertEquals(1, tripUpdates.size());
            
            addFeedHeader(feedMessage, 0);
            feedMessage.clearEntity();
            entity = createEntity("FT1");
            rtTripUpdate = createTripUpdate(tripId.getId(), startDate, GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
            entity.setTripUpdate(rtTripUpdate);
            feedMessage.addEntity(entity);
            
            tripUpdates = streamer.getUpdates();
            assertEquals(0, tripUpdates.size());
        }
        
        { // filter on entity + timestamp (none given)
            addFeedHeader(feedMessage, 20);
            feedMessage.clearEntity();
            entity = createEntity("FTG1");
            rtTripUpdate = createTripUpdate(tripId.getId(), startDate, GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
            entity.setTripUpdate(rtTripUpdate);
            feedMessage.addEntity(entity);
            
            tripUpdates = streamer.getUpdates();
            assertEquals(1, tripUpdates.size());
            
            addFeedHeader(feedMessage, 30);
            feedMessage.clearEntity();
            entity = createEntity("FTG1");
            rtTripUpdate = createTripUpdate(tripId.getId(), startDate, GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
            entity.setTripUpdate(rtTripUpdate);
            feedMessage.addEntity(entity);
            
            tripUpdates = streamer.getUpdates();
            assertEquals(1, tripUpdates.size());
        }
        
        { // filter on entity + timestamp (given)
            addFeedHeader(feedMessage, 40);
            feedMessage.clearEntity();
            entity = createEntity("F3");
            rtTripUpdate = createTripUpdate(tripId.getId(), startDate, GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
            rtTripUpdate.setTimestamp(6);
            entity.setTripUpdate(rtTripUpdate);
            feedMessage.addEntity(entity);
            
            tripUpdates = streamer.getUpdates();
            assertEquals(1, tripUpdates.size());
            
            addFeedHeader(feedMessage, 50);
            feedMessage.clearEntity();
            entity = createEntity("F3");
            rtTripUpdate = createTripUpdate(tripId.getId(), startDate, GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
            rtTripUpdate.setTimestamp(10);
            entity.setTripUpdate(rtTripUpdate);
            feedMessage.addEntity(entity);
            
            tripUpdates = streamer.getUpdates();
            assertEquals(1, tripUpdates.size());
            
            addFeedHeader(feedMessage, 60);
            feedMessage.clearEntity();
            entity = createEntity("F3");
            rtTripUpdate = createTripUpdate(tripId.getId(), startDate, GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
            rtTripUpdate.setTimestamp(5);
            entity.setTripUpdate(rtTripUpdate);
            feedMessage.addEntity(entity);
            
            tripUpdates = streamer.getUpdates();
            assertEquals(0, tripUpdates.size());
        }
    }
    
    @Test
    public void testGetUpdateForCanceledTrip() {
        TripUpdate tripUpdate = null;
        GtfsRealtime.TripUpdate.Builder rtTripUpdate;

        rtTripUpdate = createTripUpdate(tripId.getId(), startDate, GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED);
        tripUpdate = streamer.getUpdateForCanceledTrip(tripId, rtTripUpdate.build(), timestamp, serviceDate);
        assertNotNull(tripUpdate);
        assertEquals(TripUpdate.Status.CANCELED, tripUpdate.getStatus());
    }
    
    @Test
    public void testGetUpdateForAddedTrip() {
        TripUpdate tripUpdate = null;
        GtfsRealtime.TripUpdate.Builder rtTripUpdate;
        
        // no route id
        rtTripUpdate = createTripUpdate(tripId.getId(), startDate);
        tripUpdate = streamer.getUpdateForAddedTrip(tripId, rtTripUpdate.build(), timestamp, serviceDate);
        addStopTimeUpdate(rtTripUpdate, stopId.getId(), today, 0, null);
        addStopTimeUpdate(rtTripUpdate, stopId.getId(), today, 1, null);
        assertNull(tripUpdate);

        rtTripUpdate = createTripUpdate(tripId.getId(), startDate, routeId);
        
        // no stop times
        tripUpdate = streamer.getUpdateForAddedTrip(tripId, rtTripUpdate.build(), timestamp, serviceDate);
        assertNull(tripUpdate);

        // single stop time
        addStopTimeUpdate(rtTripUpdate, stopId.getId(), today, 0, null);
        tripUpdate = streamer.getUpdateForAddedTrip(tripId, rtTripUpdate.build(), timestamp, serviceDate);
        assertNull(tripUpdate);
        
        // second stop time
        addStopTimeUpdate(rtTripUpdate, stopId.getId(), today, 1, null);
        tripUpdate = streamer.getUpdateForAddedTrip(tripId, rtTripUpdate.build(), timestamp, serviceDate);
        assertNotNull(tripUpdate);
        assertEquals(TripUpdate.Status.ADDED, tripUpdate.getStatus());

        // wheelchair accessibility
        GtfsRealtime.VehicleDescriptor.Builder vehicle = GtfsRealtime.VehicleDescriptor.newBuilder();
        vehicle.setExtension(GtfsRealtime.wheelchairAccessible, TripTimes.WHEELCHAIR_ACCESSIBLE);
        rtTripUpdate.setVehicle(vehicle);

        assertEquals(new Integer(0), tripUpdate.getWheelchairAccessible()); // default
        tripUpdate = streamer.getUpdateForAddedTrip(tripId, rtTripUpdate.build(), timestamp, serviceDate);
        assertTrue(tripUpdate.getWheelchairAccessible() == TripTimes.WHEELCHAIR_ACCESSIBLE);
    }
    
    @Test
    public void testGetUpdateForScheduledTrip() {
        TripUpdate tripUpdate = null;
        GtfsRealtime.TripUpdate.Builder rtTripUpdate = createTripUpdate(tripId.getId(), startDate);

        // no stop times
        tripUpdate = streamer.getUpdateForScheduledTrip(tripId, rtTripUpdate.build(), timestamp, serviceDate);
        assertNull(tripUpdate);

        // single stop time
        addStopTimeUpdate(rtTripUpdate, stopId.getId(), today, 0, null);
        tripUpdate = streamer.getUpdateForScheduledTrip(tripId, rtTripUpdate.build(), timestamp, serviceDate);
        assertNotNull(tripUpdate);
        assertNull(tripUpdate.getWheelchairAccessible());
        assertEquals(TripUpdate.Status.MODIFIED, tripUpdate.getStatus());

        // wheelchair accessibility
        GtfsRealtime.VehicleDescriptor.Builder vehicle = GtfsRealtime.VehicleDescriptor.newBuilder();
        vehicle.setExtension(GtfsRealtime.wheelchairAccessible, TripTimes.WHEELCHAIR_ACCESSIBLE);
        rtTripUpdate.setVehicle(vehicle);

        tripUpdate = streamer.getUpdateForScheduledTrip(tripId, rtTripUpdate.build(), timestamp, serviceDate);
        assertTrue(tripUpdate.getWheelchairAccessible() == TripTimes.WHEELCHAIR_ACCESSIBLE);
    }
    
    @Test
    public void testGetStopTimeUpdateForTrip() {

        Update update = null;
        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate
            = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
        
        update = streamer.getStopTimeUpdateForTrip(tripId, timestamp, serviceDate, stopTimeUpdate.build());
        assertNull(update);
        
        stopTimeUpdate.setScheduleRelationship(
                GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA);
        
        update = streamer.getStopTimeUpdateForTrip(tripId, timestamp, serviceDate, stopTimeUpdate.build());
        assertNull(update);
        
        stopTimeUpdate.setStopId(stopId.getId());
        update = streamer.getStopTimeUpdateForTrip(tripId, timestamp, serviceDate, stopTimeUpdate.build());
        assertNotNull(update);
        assertEquals(Update.Status.PLANNED, update.getStatus());
        
        stopTimeUpdate.setScheduleRelationship(
                GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);
        update = streamer.getStopTimeUpdateForTrip(tripId, timestamp, serviceDate, stopTimeUpdate.build());
        assertNotNull(update);
        assertEquals(Update.Status.CANCEL, update.getStatus());

        stopTimeUpdate.setScheduleRelationship(
                GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED);
        update = streamer.getStopTimeUpdateForTrip(tripId, timestamp, serviceDate, stopTimeUpdate.build());
        assertNull(update);

        setStopTimeEvent(true, stopTimeUpdate, 0, null);
        stopTimeUpdate.setScheduleRelationship(
                GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED);
        update = streamer.getStopTimeUpdateForTrip(tripId, timestamp, serviceDate, stopTimeUpdate.build());
        assertNull(update);

        setStopTimeEvent(true, stopTimeUpdate, null, today + 10 * 60);
        update = streamer.getStopTimeUpdateForTrip(tripId, timestamp, serviceDate, stopTimeUpdate.build());
        assertNotNull(update);
        assertEquals(Update.Status.PREDICTION, update.getStatus());
        assertEquals(10 * 60, update.arrive);
        assertEquals(10 * 60, update.depart);

        setStopTimeEvent(false, stopTimeUpdate, null, today + 20 * 60);
        update = streamer.getStopTimeUpdateForTrip(tripId, timestamp, serviceDate, stopTimeUpdate.build());
        assertNotNull(update);
        assertEquals(10 * 60, update.arrive);
        assertEquals(20 * 60, update.depart);

        setStopTimeEvent(true, stopTimeUpdate, null, null);
        update = streamer.getStopTimeUpdateForTrip(tripId, timestamp, serviceDate, stopTimeUpdate.build());
        assertNotNull(update);
        assertEquals(20 * 60, update.arrive);
        assertEquals(20 * 60, update.depart);

        setStopTimeEvent(false, stopTimeUpdate, null, null);
        update = streamer.getStopTimeUpdateForTrip(tripId, timestamp, serviceDate, stopTimeUpdate.build());
        assertNull(update);
    }

    @Test
    public void testValidateTripDescriptor() {
        GtfsRealtimeHttpUpdateStreamer streamer = new GtfsRealtimeHttpUpdateStreamer();
        
        GtfsRealtime.TripDescriptor.Builder builder = GtfsRealtime.TripDescriptor.newBuilder();
        assertFalse(streamer.validateTripDescriptor(builder.build()));

        builder.setStartDate(startDate);
        assertFalse(streamer.validateTripDescriptor(builder.build()));

        builder.setTripId(tripId.getId());
        assertTrue(streamer.validateTripDescriptor(builder.build()));

        builder.clearStartDate();
        assertTrue(streamer.validateTripDescriptor(builder.build()));

        builder.setStartTime("20:20:20");
        assertFalse(streamer.validateTripDescriptor(builder.build()));
    }

    // ** UTILITY FUNCTIONS ** //

    private GtfsRealtime.FeedEntity.Builder createEntity(String id) {
        GtfsRealtime.FeedEntity.Builder entity = GtfsRealtime.FeedEntity.newBuilder();
        entity.setId(id);
        return entity;
    }
    
    private void addFeedHeader(GtfsRealtime.FeedMessage.Builder feedMessage) {
        addFeedHeader(feedMessage, new Date().getTime() / 1000);
    }
    
    private void addFeedHeader(GtfsRealtime.FeedMessage.Builder feedMessage, long timestamp) {
        GtfsRealtime.FeedHeader.Builder header = GtfsRealtime.FeedHeader.newBuilder();
        header.setGtfsRealtimeVersion("1.0");
        header.setTimestamp(timestamp);
        feedMessage.setHeader(header);
    }
    
    private GtfsRealtime.TripUpdate.Builder createTripUpdate(String tripId, String startDate) {
        GtfsRealtime.TripUpdate.Builder tripUpdate = GtfsRealtime.TripUpdate.newBuilder();
        GtfsRealtime.TripDescriptor.Builder builder = GtfsRealtime.TripDescriptor.newBuilder();
        builder.setTripId(tripId);
        builder.setStartDate(startDate);
        tripUpdate.setTrip(builder);
        return tripUpdate;
    }
    
    private GtfsRealtime.TripUpdate.Builder createTripUpdate(String tripId, String startDate, GtfsRealtime.TripDescriptor.ScheduleRelationship sr) {
        GtfsRealtime.TripUpdate.Builder tripUpdate = GtfsRealtime.TripUpdate.newBuilder();
        GtfsRealtime.TripDescriptor.Builder builder = GtfsRealtime.TripDescriptor.newBuilder();
        builder.setTripId(tripId);
        builder.setStartDate(startDate);
        builder.setScheduleRelationship(sr);
        tripUpdate.setTrip(builder);
        return tripUpdate;
    }

    private GtfsRealtime.TripUpdate.Builder createTripUpdate(String tripId, String startDate, String routeId) {
        GtfsRealtime.TripUpdate.Builder tripUpdate = GtfsRealtime.TripUpdate.newBuilder();
        GtfsRealtime.TripDescriptor.Builder builder = GtfsRealtime.TripDescriptor.newBuilder();
        builder.setTripId(tripId);
        builder.setRouteId(routeId);
        builder.setStartDate(startDate);
        builder.setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.UNSCHEDULED);
        tripUpdate.setTrip(builder);
        return tripUpdate;
    }
        
    private GtfsRealtime.TripUpdate.Builder createTripUpdate(String tripId, String startDate, String routeId, GtfsRealtime.TripDescriptor.ScheduleRelationship sr) {
        GtfsRealtime.TripUpdate.Builder tripUpdate = GtfsRealtime.TripUpdate.newBuilder();
        GtfsRealtime.TripDescriptor.Builder builder = GtfsRealtime.TripDescriptor.newBuilder();
        builder.setTripId(tripId);
        builder.setRouteId(routeId);
        builder.setStartDate(startDate);
        builder.setScheduleRelationship(sr);
        tripUpdate.setTrip(builder);
        return tripUpdate;
    }
    
    private void addStopTimeUpdate(GtfsRealtime.TripUpdate.Builder tripUpdate, String stopId, long time, int seq, GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship sr) {
        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder builder = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
        builder.setStopId(stopId);
        builder.setStopSequence(seq);
        builder.setArrival(getStopTimeEvent(time));
        builder.setDeparture(getStopTimeEvent(time));
        if(sr != null)
            builder.setScheduleRelationship(sr);
        tripUpdate.addStopTimeUpdate(builder);
    }

    private void setStopTimeEvent(boolean arrival, GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdate, Integer delay, Long time) {
        GtfsRealtime.TripUpdate.StopTimeEvent.Builder builder = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder();
        if(delay != null) {
            builder.setDelay(delay);
        }
        if(time != null) {
            builder.setTime(time);
        }
        
        if(arrival) {
            stopTimeUpdate.setArrival(builder);
        } else {
            stopTimeUpdate.setDeparture(builder);
        }
    }
    
    private GtfsRealtime.TripUpdate.StopTimeEvent.Builder getStopTimeEvent(long time) {
        GtfsRealtime.TripUpdate.StopTimeEvent.Builder builder = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder();
        builder.setTime(time);
        return builder;
    }
    
    private GtfsRealtime.TranslatedString.Builder getTranslation(String text) {
        GtfsRealtime.TranslatedString.Builder builder = GtfsRealtime.TranslatedString.newBuilder();
        builder.addTranslation(getString(text));
        return builder;
    }
    
    private GtfsRealtime.TranslatedString.Translation.Builder getString(String text) {
        GtfsRealtime.TranslatedString.Translation.Builder builder = GtfsRealtime.TranslatedString.Translation.newBuilder();
        builder.setText(text);
        return builder;
    }
    
    private GtfsRealtime.EntitySelector.Builder getInformedRouteEntity(String agencyId, String routeId) {
        GtfsRealtime.EntitySelector.Builder builder = GtfsRealtime.EntitySelector.newBuilder();
        builder.setAgencyId(agencyId);
        builder.setRouteId(routeId);
        return builder;
    }
    
    private GtfsRealtime.EntitySelector.Builder getInformedStopEntity(String agencyId, String stopId) {
        GtfsRealtime.EntitySelector.Builder builder = GtfsRealtime.EntitySelector.newBuilder();
        builder.setAgencyId(agencyId);
        builder.setStopId(stopId);
        return builder;
    }
    
    private GtfsRealtime.TimeRange.Builder getTimeRange(long start, long end) {
        GtfsRealtime.TimeRange.Builder builder = GtfsRealtime.TimeRange.newBuilder();
        builder.setStart(start);
        builder.setEnd(end);
        return builder;
    }

    
    @Before
    public void setUp() {
        streamer = new GtfsRealtimeAbstractUpdateStreamer() {
            @Override
            protected GtfsRealtime.FeedMessage getFeedMessage() {
                return feedMessage == null ? null : feedMessage.build();
            }
        };
        streamer.setDefaultAgencyId(agencyId);
        streamer.setTransitIndexService(new TransitIndexService() {
            @Override
            public Map<AgencyAndId, Route> getAllRoutes() {
                return Collections.singletonMap(new AgencyAndId(agencyId, routeId), new Route());
            }
            @Override
            public void remove(Result result) {}
            @Override
            public List<RouteVariant> getVariantsForRoute(AgencyAndId route) {return null;}
            @Override
            public List<RouteVariant> getVariantsForAgency(String agency) {return null;}
            @Override
            public RouteVariant getVariantForTrip(AgencyAndId trip) {return null;}
            @Override
            public Collection<Stop> getStopsForRoute(AgencyAndId route) {return null;}
            @Override
            public List<AgencyAndId> getRoutesForStop(AgencyAndId stop) {return null;}
            @Override
            public PreBoardEdge getPreBoardEdge(AgencyAndId stop) {return null;}
            @Override
            public PreAlightEdge getPreAlightEdge(AgencyAndId stop) {return null;}
            @Override
            public TableTripPattern getPatternForTrip(AgencyAndId tripId) {return null;}
            @Override
            public int getOvernightBreak() {return 0;}
            @Override
            public Collection<String> getDirectionsForRoute(AgencyAndId route) {return null;}
            @Override
            public Coordinate getCenter() {return null;}
            @Override
            public List<ServiceCalendar> getCalendarsByAgency(String agency) {return null;}
            @Override
            public List<ServiceCalendarDate> getCalendarDatesByAgency(String agency) {return null;}
            @Override
            public Map<AgencyAndId, Stop> getAllStops() {return null;}
            @Override
            public Collection<AgencyAndId> getAllRouteIds() {return null;}
            @Override
            public List<TraverseMode> getAllModes() {return null;}
            @Override
            public List<String> getAllAgencies() {return null;}
            @Override
            public Agency getAgency(String id) {return null;}
            @Override
            public void addCalendars(Collection<ServiceCalendar> allCalendars) {}
            @Override
            public void addCalendarDates(Collection<ServiceCalendarDate> allDates) {}
            @Override
            public void add(Result result) {}
       });
    }
}
