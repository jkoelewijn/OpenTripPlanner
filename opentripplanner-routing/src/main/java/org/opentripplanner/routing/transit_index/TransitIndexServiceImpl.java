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

package org.opentripplanner.routing.transit_index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.onebusaway.gtfs.model.Agency;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.Route;
import org.onebusaway.gtfs.model.ServiceCalendar;
import org.onebusaway.gtfs.model.ServiceCalendarDate;
import org.onebusaway.gtfs.model.Stop;
import org.onebusaway.gtfs.model.Trip;
import org.opentripplanner.routing.core.TraverseMode;
import org.opentripplanner.routing.edgetype.PatternDwell;
import org.opentripplanner.routing.edgetype.PatternHop;
import org.opentripplanner.routing.edgetype.PreAlightEdge;
import org.opentripplanner.routing.edgetype.PreBoardEdge;
import org.opentripplanner.routing.edgetype.TableTripPattern;
import org.opentripplanner.routing.edgetype.TransitBoardAlight;
import org.opentripplanner.routing.edgetype.factory.GTFSPatternHopFactory;
import org.opentripplanner.routing.graph.Edge;
import org.opentripplanner.routing.services.TransitIndexService;
import org.opentripplanner.routing.vertextype.TransitStopArrive;
import org.opentripplanner.routing.vertextype.TransitStopDepart;
import org.opentripplanner.util.MapUtils;

import com.vividsolutions.jts.geom.Coordinate;

public class TransitIndexServiceImpl implements TransitIndexService, Serializable {
    private static final long serialVersionUID = -8147894489513820239L;

    private HashMap<String, List<RouteVariant>> variantsByAgency;

    private HashMap<AgencyAndId, List<RouteVariant>> variantsByRoute;

    private HashMap<AgencyAndId, RouteVariant> variantsByTrip;

    private HashMap<AgencyAndId, PreAlightEdge> preAlightEdges;

    private HashMap<AgencyAndId, PreBoardEdge> preBoardEdges;
    
    private HashMap<AgencyAndId, HashSet<String>> directionsForRoute;

    private HashMap<AgencyAndId, HashSet<Stop>> stopsForRoute;
    
    private HashMap<AgencyAndId, TableTripPattern> patternForTrip;

    private List<TraverseMode> modes;

    private HashMap<String, List<ServiceCalendar>> calendarsByAgency = new HashMap<String, List<ServiceCalendar>>();

    private HashMap<String, List<ServiceCalendarDate>> calendarDatesByAgency = new HashMap<String, List<ServiceCalendarDate>>();

    private HashMap<String, Agency> agencies = new HashMap<String, Agency>();

    private HashMap<AgencyAndId, Stop> stops = new HashMap<AgencyAndId, Stop>();

    private HashMap<AgencyAndId, Route> routes = new HashMap<AgencyAndId, Route>();
    
    private Coordinate center;

    private int overnightBreak;

    public TransitIndexServiceImpl(HashMap<String, List<RouteVariant>> variantsByAgency,
            HashMap<AgencyAndId, List<RouteVariant>> variantsByRoute,
            HashMap<AgencyAndId, RouteVariant> variantsByTrip,
            HashMap<AgencyAndId, PreBoardEdge> preBoardEdges,
            HashMap<AgencyAndId, PreAlightEdge> preAlightEdges,
            HashMap<AgencyAndId, TableTripPattern> tableTripPatternsByTrip,
            HashMap<AgencyAndId, HashSet<String>> directionsByRoute,
            HashMap<AgencyAndId, HashSet<Stop>> stopsByRoute,
            HashMap<AgencyAndId, TableTripPattern> patternForTrip,
            HashMap<AgencyAndId, Route> routes,
            HashMap<AgencyAndId, Stop> stops,
            List<TraverseMode> modes) {
        this.variantsByAgency = variantsByAgency;
        this.variantsByRoute = variantsByRoute;
        this.variantsByTrip = variantsByTrip;
        this.preBoardEdges = preBoardEdges;
        this.preAlightEdges = preAlightEdges;
        this.directionsForRoute = directionsByRoute;
        this.stopsForRoute = stopsByRoute;
        this.patternForTrip = patternForTrip;
        this.routes = routes;
        this.stops = stops;
        this.modes = modes;
    }

    public void merge(HashMap<String, List<RouteVariant>> variantsByAgency,
            HashMap<AgencyAndId, List<RouteVariant>> variantsByRoute,
            HashMap<AgencyAndId, RouteVariant> variantsByTrip,
            HashMap<AgencyAndId, PreBoardEdge> preBoardEdges,
            HashMap<AgencyAndId, PreAlightEdge> preAlightEdges,
            HashMap<AgencyAndId, TableTripPattern> tableTripPatternsByTrip,
            HashMap<AgencyAndId, HashSet<String>> directionsByRoute,
            HashMap<AgencyAndId, HashSet<Stop>> stopsByRoute,
            HashMap<AgencyAndId, TableTripPattern> patternForTrip,
            HashMap<AgencyAndId, Route> routes,
            HashMap<AgencyAndId, Stop> stops,
            List<TraverseMode> modes) {

        MapUtils.mergeInUnique(this.variantsByAgency, variantsByAgency);
        MapUtils.mergeInUnique(this.variantsByRoute, variantsByRoute);
        this.variantsByTrip.putAll(variantsByTrip);
        this.preBoardEdges.putAll(preBoardEdges);
        this.preAlightEdges.putAll(preAlightEdges);
        MapUtils.mergeInUnique(this.directionsForRoute, directionsByRoute);
        MapUtils.mergeInUnique(this.stopsForRoute, stopsByRoute);
        this.patternForTrip.putAll(patternForTrip);
        this.routes.putAll(routes);
        this.stops.putAll(stops);
        for (TraverseMode mode : modes) {
            if (!this.modes.contains(mode)) {
                this.modes.add(mode);
            }
        }
    }
    
    @Override
    public void add(GTFSPatternHopFactory.Result result) {
        RouteVariant variant = findVariantForResult(result);
        if(variant != null) {
            variant.addTrip(result.trip, 1);

            variantsByTrip.put(result.trip.getId(), variant);
            patternForTrip.put(result.trip.getId(), result.tripPattern);
            return;
        }
        
        variant = createRouteVariant(result);

        synchronized (this) {
            MapUtils.addToMapList(variantsByAgency, result.trip.getId().getAgencyId(), variant);
            MapUtils.addToMapList(variantsByRoute, result.trip.getRoute().getId(), variant);
            // TODO: directionsByRoute
            // TODO: stopsByRoute
            variantsByTrip.put(result.trip.getId(), variant);
            patternForTrip.put(result.trip.getId(), result.tripPattern);
        }
    }
    
    private RouteVariant findVariantForResult(GTFSPatternHopFactory.Result result) {
        AgencyAndId routeId = result.trip.getRoute().getId();
        for(RouteVariant variant : getVariantsForRoute(routeId)) {
            if(variant.getStops().equals(result.stopPattern.getStops())) {
                return variant;
            }
        }
        return null;
    }
    
    private RouteVariant createRouteVariant(GTFSPatternHopFactory.Result result) {
        RouteVariant variant = new RouteVariant(result.trip.getRoute(), result.stopPattern.getStops());
        variant.addTrip(result.trip, 1);
        variant.setName("for " + result.trip.getId()); // TODO: name

        // TODO: interlines
        // TODO: geometry

        Map<AgencyAndId, RouteSegment> stopSegmentMap = new HashMap<AgencyAndId, RouteSegment>();
        for(Edge e : result.edges) {
            // public Edge dwell;

            if(e instanceof TransitBoardAlight) {
                TransitBoardAlight tba = (TransitBoardAlight) e;
                if(tba.isBoarding()) {
                    AgencyAndId stopId = ((TransitStopDepart) tba.getFromVertex()).getStopId();
                    getOrCreateSegmentForStop(stopId, stopSegmentMap).board = tba;
                } else {
                    AgencyAndId stopId = ((TransitStopArrive) tba.getToVertex()).getStopId();
                    getOrCreateSegmentForStop(stopId, stopSegmentMap).alight = tba;
                }
            }
            else if(e instanceof PatternDwell) {
                PatternDwell pd = (PatternDwell) e;
                AgencyAndId stopId = pd.getPattern().getStops().get(pd.getStopIndex()).getId();
                getOrCreateSegmentForStop(stopId, stopSegmentMap).dwell = pd;
            }
            else if(e instanceof PatternHop) {
                PatternHop ph = (PatternHop) e;
                getOrCreateSegmentForStop(ph.getStartStop().getId(), stopSegmentMap).hopOut = ph;
                getOrCreateSegmentForStop(ph.getEndStop().getId(), stopSegmentMap).hopIn = ph;
            }
        }
        
        for(RouteSegment segment : stopSegmentMap.values()) {
            variant.addSegment(segment);
            variant.addExemplarSegment(segment);
        }
        
        return variant;
    }
    
    private RouteSegment getOrCreateSegmentForStop(AgencyAndId stopId, Map<AgencyAndId, RouteSegment> stopSegmentMap) {
        if(stopSegmentMap.containsKey(stopId)) {
            return stopSegmentMap.get(stopId);
        } else {
            RouteSegment segment = new RouteSegment(stopId);
            stopSegmentMap.put(stopId, segment);
            return segment;
        }
    }
    
    @Override
    public void remove(GTFSPatternHopFactory.Result result) {
        RouteVariant variant = variantsByTrip.get(result.trip.getId());
        synchronized (this) {
            variant.removeTrip(result.trip);
            variantsByTrip.remove(result.trip.getId());
            patternForTrip.remove(result.trip.getId()); 

            if(variant.getTrips().isEmpty()) {
                MapUtils.removeFromMapList(variantsByAgency, result.trip.getId().getAgencyId(), variant);
                MapUtils.removeFromMapList(variantsByRoute, result.trip.getRoute().getId(), variant);
                // TODO: directionsByRoute
                // TODO: stopsByRoute  
            }
        }
    }

    @Override
    public List<RouteVariant> getVariantsForAgency(String agency) {
        List<RouteVariant> variants = variantsByAgency.get(agency);
        if (variants == null) {
            return Collections.emptyList();
        }
        return variants;
    }

    @Override
    public List<RouteVariant> getVariantsForRoute(AgencyAndId route) {
        List<RouteVariant> variants = variantsByRoute.get(route);
        if (variants == null) {
            return Collections.emptyList();
        }
        return variants;
    }

    @Override
    public RouteVariant getVariantForTrip(AgencyAndId trip) {
        return variantsByTrip.get(trip);
    }

    @Override
    public PreAlightEdge getPreAlightEdge(AgencyAndId stop) {
        return preAlightEdges.get(stop);
    }
    
    @Override
    public PreBoardEdge getPreBoardEdge(AgencyAndId stop) {
        return preBoardEdges.get(stop);
    }

    @Override
    public Collection<String> getDirectionsForRoute(AgencyAndId route) {
        return directionsForRoute.get(route);
    }

    @Override
    public List<TraverseMode> getAllModes() {
        return modes;
    }

    @Override
    public List<String> getAllAgencies() {
        return new ArrayList<String>(variantsByAgency.keySet());
    }

    @Override
    public Collection<AgencyAndId> getAllRouteIds() {
        return variantsByRoute.keySet();
    }

    @Override
    public Map<AgencyAndId, Route> getAllRoutes() {
        return Collections.unmodifiableMap(routes);
    }

    @Override
    public Map<AgencyAndId, Stop> getAllStops() {
        return Collections.unmodifiableMap(stops);
    }

    @Override
    public void addCalendars(Collection<ServiceCalendar> allCalendars) {
        for (ServiceCalendar calendar : allCalendars) {
            MapUtils.addToMapList(calendarsByAgency, calendar.getServiceId().getAgencyId(),
                    calendar);
        }
    }

    @Override
    public void addCalendarDates(Collection<ServiceCalendarDate> allDates) {
        for (ServiceCalendarDate date : allDates) {
            MapUtils.addToMapList(calendarDatesByAgency, date.getServiceId().getAgencyId(), date);
        }
    }

    @Override
    public List<ServiceCalendarDate> getCalendarDatesByAgency(String agency) {
        return calendarDatesByAgency.get(agency);
    }

    @Override
    public List<ServiceCalendar> getCalendarsByAgency(String agency) {
        return calendarsByAgency.get(agency);
    }

    @Override
    public Agency getAgency(String id) {
        return agencies.get(id);
    }

    public void addAgency(Agency agency) {
        agencies.put(agency.getId(), agency);
    }

    @Override
    public List<AgencyAndId> getRoutesForStop(AgencyAndId stop) {
        HashSet<AgencyAndId> out = new HashSet<AgencyAndId>();
        Edge edge = preBoardEdges.get(stop);
        if (edge == null)
            return new ArrayList<AgencyAndId>();
        for (Edge e: edge.getToVertex().getOutgoing()) {
            if (e instanceof TransitBoardAlight && ((TransitBoardAlight) e).isBoarding()) {
                TransitBoardAlight board = (TransitBoardAlight) e;
                for (Trip t : board.getPattern().getTrips()) {
                    out.add(t.getRoute().getId());
                }
            }
        }
        return new ArrayList<AgencyAndId>(out);
    }

    public void setCenter(Coordinate coord) {
        this.center = coord;
    }

    @Override
    public Coordinate getCenter() {
        return center;
    }

    public void setOvernightBreak(int overnightBreak) {
        this.overnightBreak = overnightBreak;
    }

    @Override
    public int getOvernightBreak() {
        return overnightBreak;
    }

    @Override
    public Collection<Stop> getStopsForRoute(AgencyAndId route) {
        return stopsForRoute.get(route);
    }

    @Override
    public TableTripPattern getPatternForTrip(AgencyAndId tripId) {
        return patternForTrip.get(tripId);
    }
}
