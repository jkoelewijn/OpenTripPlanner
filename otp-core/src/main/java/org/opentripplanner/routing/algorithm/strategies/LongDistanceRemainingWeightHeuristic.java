/* This program is free software: you can redistribute it and/or
 modify it under the terms of the GNU Lesser General Public License
 as published by the Free Software Foundation, either version 3 of
 the License, or (props, at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package org.opentripplanner.routing.algorithm.strategies;

import org.opentripplanner.common.geometry.DistanceLibrary;
import org.opentripplanner.common.geometry.SphericalDistanceLibrary;
import org.opentripplanner.routing.core.State;
import org.opentripplanner.routing.core.RoutingRequest;
import org.opentripplanner.routing.edgetype.StreetTransitLink;
import org.opentripplanner.routing.graph.Vertex;
import org.opentripplanner.routing.impl.LongDistancePathService.Parser;
import org.opentripplanner.routing.vertextype.TransitVertex;

/**
 * A Euclidean remaining weight strategy that takes into account the property of the long distance
 * path service that when transit has been alighted, it is not possible to board the transit again.
 * 
 */
public class LongDistanceRemainingWeightHeuristic implements RemainingWeightHeuristic {

    // Assume that the max average transit speed is 33 m/s, which is roughly
    // true in the Netherlands
    private static final int MAX_TRANSIT_SPEED = 33;

    private static final long serialVersionUID = 6282586254479335065L;

    private RoutingRequest options;

    private boolean useTransit = false;

    private DistanceLibrary distanceLibrary = SphericalDistanceLibrary.getInstance();

    private Parser parser;

    @Override
    public void initialize(State s, Vertex target) {
        this.options = s.getOptions();
        this.useTransit = options.getModes().isTransit();

        // Create a long distance path parser to determine street type
        parser = new Parser();
    }

    /**
     * On a non-transit trip, the remaining weight is simply distance / speed.
     * On a transit trip, there are two cases: 
     * (1) we alighted transit once and can't use the transit network anymore. 
     * (2) we are still able to use the transit network.
     */
    @Override
    public double computeForwardWeight(State s, Vertex target) {
        Vertex sv = s.getVertex();
        double euclideanDistance = distanceLibrary.fastDistance(sv.getY(), sv.getX(), target.getY(),
                target.getX());
        if (useTransit) {
            if (s.isEverBoarded()) {
                if (parser.terminalFor(s) == Parser.STREET) {
                    // This path has been on transit and is on the street network now
                    // It can only use the street network for the rest of the trip
                    return streetCost(euclideanDistance);
                }
                else if (s.getBackEdge() instanceof StreetTransitLink &&
                        ((StreetTransitLink)s.getBackEdge()).getFromVertex() instanceof TransitVertex) {
                    // This path has been on transit and goes to the street network now
                    // It can only use the street network for the rest of the trip
                    return streetCost(euclideanDistance);
                }
                else {
                    // This path will continue with the possibility of using transit
                    return transitCost(euclideanDistance);
                }
            } else {
                // This path will continue with the possibility of using transit
                return transitCost(euclideanDistance);
            }
        } else {
            // Search disallows using transit
            // It can only use the street network
            return streetCost(euclideanDistance);
        }
    }

    private double streetCost(double euclideanDistance) {
        double cost = walkReluctanceFactor() * euclideanDistance / options.getStreetSpeedUpperBound();
        return cost;
    }

    private double transitCost(double euclideanDistance) {
        double cost = euclideanDistance / MAX_TRANSIT_SPEED;
        return cost;
    }
    
    private double walkReluctanceFactor() {
        // Assume carSpeed > bikeSpeed > walkSpeed
        if (options.modes.getDriving())
            return 1.0;
        if (options.modes.getBicycle())
            return 1.0;
        return options.getWalkReluctance();
    }
    
    /**
     * computeForwardWeight and computeReverseWeight were identical (except that 
     * computeReverseWeight did not have the localStreetService clause). They have been merged.
     */
    @Override
    public double computeReverseWeight(State s, Vertex target) {
        return computeForwardWeight(s, target);
    }

    @Override
    public void reset() {}

    @Override
    public void doSomeWork() {}

}
