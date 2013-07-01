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

import org.opentripplanner.routing.edgetype.TableTripPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TripTimesFactory {
    private static final Logger LOG = LoggerFactory.getLogger(TripTimesFactory.class);

    public static TripTimes create(TableTripPattern pattern, TripUpdate tripUpdate, ScheduledTripTimes scheduledTimes) {
        TripTimes newTimes = null;
    
        if(tripUpdate.isRemoval()) {
            return scheduledTimes;
        } else if (tripUpdate.isCancellation()) {
            newTimes = new CanceledTripTimes(scheduledTimes);
        }
        else {
            // 'stop' Index as in transit stop (not 'end', not 'hop')
            int stopIndex = tripUpdate.findUpdateStopIndex(pattern);
            if (stopIndex == TripUpdate.MATCH_FAILED) {
                LOG.warn("Unable to match update block to stopIds.");
                return null;
            }
            
            newTimes = new UpdatedTripTimes(scheduledTimes, tripUpdate, stopIndex);
            if ( ! newTimes.timesIncreasing()) {
                LOG.warn("Resulting UpdatedTripTimes has non-increasing times. " +
                         "Falling back on DecayingDelayTripTimes.");
                LOG.warn(tripUpdate.toString());
                LOG.warn(newTimes.toString());
                int delay = newTimes.getDepartureDelay(stopIndex);
                // maybe decay should be applied on top of the update (wrap Updated in Decaying), 
                // starting at the end of the update block
                newTimes = new DecayingDelayTripTimes(scheduledTimes, stopIndex, delay);
                LOG.warn(newTimes.toString());
                if ( ! newTimes.timesIncreasing()) {
                    LOG.error("Even these trip times are non-increasing. Underlying schedule problem?");
                    return null;
                }
            }
        }

        return newTimes;
    }
}
