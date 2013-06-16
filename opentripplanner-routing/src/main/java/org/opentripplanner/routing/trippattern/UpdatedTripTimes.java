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

/**
 * An UpdatedTripTimes applies updated arrival and departure times to a subset of a trip's stops,
 * reading through to the scheduled TripTimes for subsequent stops, and reporting that the vehicle
 * has passed stops preceding the update block.
 */
public class UpdatedTripTimes extends DelegatingTripTimes {

    private final int offset;

    private int[] arrivals;

    private int[] departures;

    private int[] perStopFlags;
    
    private boolean wheelchairAccessible;

    // maybe push pattern and offset into block
    public UpdatedTripTimes(ScheduledTripTimes sched, TripUpdate tripUpdate, int offset) {
        super(sched);
        this.wheelchairAccessible = tripUpdate.getWheelchairAccessible() == null
                                  ? sched.isWheelchairAccessible()
                                  : tripUpdate.getWheelchairAccessible() == TripTimes.WHEELCHAIR_ACCESSIBLE;
        this.offset = offset;
        int nUpdates = tripUpdate.getUpdates().size();
        this.arrivals = new int[nUpdates];
        this.departures = new int[nUpdates];
        this.perStopFlags = new int[nUpdates];
        int ui = 0;
        for (Update update : tripUpdate.getUpdates()) {
            switch (update.status) {
            case PASSED:
                perStopFlags[ui] |= NO_PICKUP << SHIFT_PICKUP;
                perStopFlags[ui] |= NO_DROPOFF << SHIFT_DROPOFF;
                arrivals[ui] = TripTimes.PASSED;
                departures[ui] = TripTimes.PASSED;
                break;
            case CANCEL:
                perStopFlags[ui] |= NO_PICKUP << SHIFT_PICKUP;
                perStopFlags[ui] |= NO_DROPOFF << SHIFT_DROPOFF;
                arrivals[ui] = TripTimes.CANCELED;
                departures[ui] = TripTimes.CANCELED;
                break;
            case UNKNOWN:
            case PLANNED:
                perStopFlags[ui] |= sched.getBoardType(ui) << SHIFT_PICKUP;
                perStopFlags[ui] |= sched.getAlightType(ui) << SHIFT_DROPOFF;
                arrivals[ui] = sched.getArrivalTime(offset + ui - 1);
                departures[ui] = sched.getDepartureTime(offset + ui);
                break;
            case ARRIVED:
            case PREDICTION:
                perStopFlags[ui] |= sched.getBoardType(ui) << SHIFT_PICKUP;
                perStopFlags[ui] |= sched.getAlightType(ui) << SHIFT_DROPOFF;
                arrivals[ui] = update.arrive;
                departures[ui] = update.depart;
                break;
            }
            ui += 1;
        }
        this.compact();
    }

    @Override public int getDepartureTime(int hop) {
        int stop = hop;
        int update = stop - offset;
        if (update < 0)
            return TripTimes.PASSED;
        if (update >= departures.length)
            return super.getDepartureTime(hop);
        return departures[update];
    }
    
    @Override public int getArrivalTime(int hop) {
        int stop = hop + 1;
        int update = stop - offset;
        if (update < 0)
            return TripTimes.PASSED;
        if (update >= departures.length)
            return super.getArrivalTime(hop);
        if (arrivals == null)
            return departures[update];
        return arrivals[update];
    }

    @Override public int getAlightType(int stopIndex) {
        if(perStopFlags == null)
            return super.getAlightType(stopIndex);

        int update = stopIndex - offset;
        if (update < 0)
            return NO_DROPOFF;
        if(update >= perStopFlags.length)
            return super.getAlightType(stopIndex);

        return (perStopFlags[update] & MASK_DROPOFF) >> SHIFT_DROPOFF;
    }

    @Override public int getBoardType(int stopIndex) {
        if(perStopFlags == null)
            return super.getBoardType(stopIndex);

        int update = stopIndex - offset;
        if (update < 0)
            return NO_PICKUP;
        if(update >= perStopFlags.length)
            return super.getBoardType(stopIndex);

        return (perStopFlags[update] & MASK_PICKUP) >> SHIFT_PICKUP;
    }
    
    @Override public boolean isWheelchairAccessible() {
        return wheelchairAccessible;
    }

    @Override public boolean compact() {
        boolean ret = compactArrivalAndDepartureTimes();
        return compactPerStopFlags() || ret;
    }

    private boolean compactArrivalAndDepartureTimes() {
        if (arrivals == null)
            return false;
        for (int i = 0; i < arrivals.length; i++) {
            if (arrivals[i] != departures[i]) {
                return false;
            }
        }
        arrivals = null;
        return true;
    }

    private boolean compactPerStopFlags() {
        if(perStopFlags == null)
            return false;

        for(int stop = offset; stop < offset + perStopFlags.length; ++stop) {
            if(getAlightType(stop) != super.getAlightType(stop) || getBoardType(stop) != super.getBoardType(stop)) {
                return false;
            }
        }

        perStopFlags = null;
        return true;
    }

    public String toString() {
        String s = String.format("UpdatedTripTimes block size %d at stop %d\n", 
                departures.length, offset);
        return s + dumpTimes() + "\nbased on:" + super.toString();
    }

}
