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

package org.opentripplanner.routing.trippattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.Stop;
import org.onebusaway.gtfs.model.StopTime;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.model.calendar.ServiceDate;
import org.opentripplanner.routing.trippattern.Update.Status;

public class UpdatedTripTimesTest {
    private static ScheduledTripTimes originalTripTimesA;
    private static ScheduledTripTimes originalTripTimesB;
    private static AgencyAndId tripId = new AgencyAndId("agency", "testtrip");
    
    private static AgencyAndId stop_a = new AgencyAndId("agency", "A"); // 0
    private static AgencyAndId stop_b = new AgencyAndId("agency", "B"); // 1
    private static AgencyAndId stop_c = new AgencyAndId("agency", "C"); // 2
    private static AgencyAndId stop_d = new AgencyAndId("agency", "D"); // 3
    private static AgencyAndId stop_e = new AgencyAndId("agency", "E"); // 4
    private static AgencyAndId stop_f = new AgencyAndId("agency", "F"); // 5
    private static AgencyAndId stop_g = new AgencyAndId("agency", "G"); // 6
    private static AgencyAndId stop_h = new AgencyAndId("agency", "H"); // 7
    private static AgencyAndId[] stops = {stop_a, stop_b, stop_c, stop_d, stop_e, stop_f, stop_g, stop_h};
    
    @BeforeClass
    public static void setUp() throws Exception {
        Trip trip = new Trip();
        trip.setId(tripId);
        
        List<StopTime> stopTimes = new LinkedList<StopTime>();
        
        for(int i =  0; i < stops.length; ++i) {
            StopTime stopTime = new StopTime();
            
            Stop stop = new Stop();
            stop.setId(stops[i]);
            stopTime.setStop(stop);
            stopTime.setArrivalTime(i * 60);
            stopTime.setDepartureTime(i * 60);
            stopTimes.add(stopTime);
        }
        stopTimes.get(1).setDropOffType(TripTimes.NO_DROPOFF);
        originalTripTimesA = new ScheduledTripTimes(trip, stopTimes);
        
        stopTimes = new LinkedList<StopTime>();
        for(int i =  0; i < stops.length; ++i) {
            StopTime stopTime = new StopTime();
            
            Stop stop = new Stop();
            stop.setId(stops[i]);
            stopTime.setStop(stop);
            stopTime.setStopSequence(i);
            stopTime.setArrivalTime(i * 60); 
            stopTime.setDepartureTime(i * 60 + (i > 3 && i < 6 ? i * 10 : 0));
            stopTimes.add(stopTime);
        }
        stopTimes.get(1).setDropOffType(TripTimes.NO_DROPOFF);
        originalTripTimesB = new ScheduledTripTimes(trip, stopTimes);
    }

    @Test
    public void testStopCancellingUpdate() {
        TripUpdate tripUpdate;
        
        List<Update> updates = new LinkedList<Update>();
        updates.add(new Update(tripId, stop_a, 0, 0, 0, Update.Status.PLANNED , 0, new ServiceDate()));
        updates.add(new Update(tripId, stop_b, 1, 0, 0, Update.Status.PLANNED , 0, new ServiceDate()));
        updates.add(new Update(tripId, stop_c, 2, 0, 0, Update.Status.CANCELED, 0, new ServiceDate()));
        updates.add(new Update(tripId, stop_d, 3, 0, 0, Update.Status.CANCELED, 0, new ServiceDate()));
        
        tripUpdate = TripUpdate.forUpdatedTrip(tripId, 0, new ServiceDate(), updates);
        
        UpdatedTripTimes updateTriptimesA = new UpdatedTripTimes(originalTripTimesA, tripUpdate, 0);

        assertTrue(updateTriptimesA.timesIncreasing());
        
        assertEquals(1 * 60, updateTriptimesA.getDepartureTime(1));
        assertEquals(2 * 60, updateTriptimesA.getDepartureTime(2));
        assertEquals(3 * 60, updateTriptimesA.getDepartureTime(3));
        assertEquals(4 * 60, updateTriptimesA.getDepartureTime(4));

        assertEquals(1 * 60, updateTriptimesA.getArrivalTime(0));
        assertEquals(2 * 60, updateTriptimesA.getArrivalTime(1));
        assertEquals(3 * 60, updateTriptimesA.getArrivalTime(2));
        assertEquals(4 * 60, updateTriptimesA.getArrivalTime(3));
        
        
        assertEquals(TripTimes.State.PLANNED , updateTriptimesA.getState(0));
        assertEquals(TripTimes.State.PLANNED , updateTriptimesA.getState(1));
        assertEquals(TripTimes.State.CANCELED, updateTriptimesA.getState(2));
        assertEquals(TripTimes.State.CANCELED, updateTriptimesA.getState(3));

        assertEquals( 60, updateTriptimesA.getRunningTime(0));
        assertEquals(  0, updateTriptimesA.getRunningTime(1));
        assertEquals(  0, updateTriptimesA.getRunningTime(2));
        assertEquals(180, updateTriptimesA.getRunningTime(3));
        assertEquals( 60, updateTriptimesA.getRunningTime(4));
        
        assertFalse(originalTripTimesA.canAlight(1));

        assertTrue (updateTriptimesA.canAlight(0));
        assertFalse(updateTriptimesA.canAlight(1));
        assertTrue (updateTriptimesA.canAlight(2));
        assertTrue (updateTriptimesA.canAlight(3));
        assertTrue (updateTriptimesA.canAlight(4));
    }

    @Test
    public void testStopUpdate() {
        TripUpdate tripUpdate;
        
        List<Update> updates = new LinkedList<Update>();
        updates.add(new Update(tripId, stop_d, 3, 190, 190, Update.Status.PREDICTION , 0, new ServiceDate()));
        
        tripUpdate = TripUpdate.forUpdatedTrip(tripId, 0, new ServiceDate(), updates);
        
        UpdatedTripTimes updateTriptimesA = new UpdatedTripTimes(originalTripTimesA, tripUpdate, 3);

        assertEquals(2 * 60     , updateTriptimesA.getDepartureTime(2));
        assertEquals(3 * 60 + 10, updateTriptimesA.getDepartureTime(3));
        assertEquals(4 * 60     , updateTriptimesA.getDepartureTime(4));

        assertEquals(2 * 60     , updateTriptimesA.getArrivalTime(1));
        assertEquals(3 * 60 + 10, updateTriptimesA.getArrivalTime(2));
        assertEquals(4 * 60     , updateTriptimesA.getArrivalTime(3));

        assertEquals(TripTimes.State.PASSED   , updateTriptimesA.getState(0));
        assertEquals(TripTimes.State.PASSED   , updateTriptimesA.getState(1));
        assertEquals(TripTimes.State.PASSED   , updateTriptimesA.getState(2));
        assertEquals(TripTimes.State.PREDICTED, updateTriptimesA.getState(3));

        assertFalse(updateTriptimesA.canAlight(1));
    }

    @Test
    public void testPassedUpdate() {
        TripUpdate tripUpdate;
        
        List<Update> updates = new LinkedList<Update>();
        updates.add(new Update(tripId, stop_a, 0, 0, 0, Update.Status.PASSED, 0, new ServiceDate()));
        
        tripUpdate = TripUpdate.forUpdatedTrip(tripId, 0, new ServiceDate(), updates);
        
        UpdatedTripTimes updateTriptimesA = new UpdatedTripTimes(originalTripTimesA, tripUpdate, 0);

        assertEquals(TripTimes.State.PASSED , updateTriptimesA.getState(0));
        assertEquals(TripTimes.State.PLANNED, updateTriptimesA.getState(1));
        
        assertEquals( 0, updateTriptimesA.getDepartureTime(0));
        assertEquals(60, updateTriptimesA.getArrivalTime(0));

        assertTrue(updateTriptimesA.canAlight(0));
        assertTrue(updateTriptimesA.canBoard(0));
        
        assertFalse(updateTriptimesA.canAlight(1));
        assertTrue(updateTriptimesA.canBoard(1));
    }


    @Test
    public void testWheelchairAccessible() {
        TripUpdate tripUpdate;
        UpdatedTripTimes updateTriptimesA;
        
        assertFalse(originalTripTimesA.isWheelchairAccessible());
        
        List<Update> updates = new LinkedList<Update>();        
        tripUpdate = TripUpdate.forUpdatedTrip(tripId, 0, new ServiceDate(), updates, new Integer(1));
        
        updateTriptimesA = new UpdatedTripTimes(originalTripTimesA, tripUpdate, 0);
        assertTrue(updateTriptimesA.isWheelchairAccessible());
        
        tripUpdate = TripUpdate.forUpdatedTrip(tripId, 0, new ServiceDate(), updates, null);
        updateTriptimesA = new UpdatedTripTimes(originalTripTimesA, tripUpdate, 0);
        assertFalse(updateTriptimesA.isWheelchairAccessible());
        
        originalTripTimesA.getTrip().setWheelchairAccessible(1);
        assertTrue(originalTripTimesA.isWheelchairAccessible());

        tripUpdate = TripUpdate.forUpdatedTrip(tripId, 0, new ServiceDate(), updates, new Integer(1));
        updateTriptimesA = new UpdatedTripTimes(originalTripTimesA, tripUpdate, 0);
        assertTrue(updateTriptimesA.isWheelchairAccessible());

        tripUpdate = TripUpdate.forUpdatedTrip(tripId, 0, new ServiceDate(), updates, new Integer(0));
        updateTriptimesA = new UpdatedTripTimes(originalTripTimesA, tripUpdate, 0);
        assertFalse(updateTriptimesA.isWheelchairAccessible());
        

        tripUpdate = TripUpdate.forUpdatedTrip(tripId, 0, new ServiceDate(), updates, null);
        updateTriptimesA = new UpdatedTripTimes(originalTripTimesA, tripUpdate, 0);
        assertTrue(updateTriptimesA.isWheelchairAccessible());
    }
}
