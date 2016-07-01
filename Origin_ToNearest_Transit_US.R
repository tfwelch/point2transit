library(foreach)
library(parallel)
library(doParallel)
library(rgeos)
library(rgdal)
library(DBI)
library(RSQLite)
library(RPostgreSQL)


setwd("R:/Bikeshare/Data/CELL_ODs_WORK_Copy")


## Set-up Inputs/OUTPUTS
origins<-readOGR(dsn = "CaBi_Archive", layer = "CaBi_Stations") #input origins shapefile
csvoutname<-"CaBi_Transit_Distance" #name of output csv


num.cores <- detectCores() ## detect how many cores on your machine
cl <- makeCluster(num.cores, type = "SOCK")
registerDoParallel(cl)
getDoParWorkers() ## check if all cores are working
clusterEvalQ(cl,library(RPostgreSQL))
clusterEvalQ(cl,library(DBI))
clusterEvalQ(cl,library(RSQLite))
clusterEvalQ(cl,library(sp))
clusterEvalQ(cl,library(rgeos))

TRANSIT_FINAL<- data.frame(
  originID=numeric(), 
  bs_x=numeric(), 
  bs_y=numeric(), 
  RailDIS_n=numeric(),
  rail_x=numeric(), 
  rail_y=numeric(),
  BusDIS_n=numeric(),
  bus_x=numeric(), 
  bus_y=numeric(),
  stringsAsFactors=FALSE) 

#Read in bike station locations
timeTR<-system.time({
  OO<-nrow(origins)

TRANSIT_FINAL=foreach(i=1:OO,.combine = rbind) %dopar% {
  
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, user = "postgres", dbname = "osm", host = "localhost")
  con_rt <- dbConnect(drv, user = "postgres", dbname = "opbeumDB", host = "localhost")

  origins_bb<-bbox(origins[i,])
    #get origin ID and YX
    originID<-as.numeric(as.character(origins$TERMINAL_N)[i]) #change this to the ID column for the loaded shape
    bbl<-origins_bb*.999  
    bbh<-origins_bb*1.001  
    O_xy<-coordinates(origins[i,])
  
  ##Find destinations in area
  select<-"SELECT 
  route_type,
  ST_Y(geom) AS lat, 
  ST_X(geom) AS lon 
  FROM gtfs_stops_us
  WHERE  
  geom && 
  ST_MakeEnvelope("
  
  bb2<-paste(bbl[1,1],",",bbl[2,1],",",bbh[1,2],",",bbh[2,2],",","4283")
  q_tsig <- paste(select,bb2,")")
  
  #Pull SQL result
  tsig_result <- dbGetQuery(con, q_tsig)

  #get only bus stations
  tsig_result_bus <- tsig_result[ which(tsig_result$route_type==3), ]
  
  #get only rail stations
  tsig_result_rail <- tsig_result[ which(tsig_result$route_type<3), ]
  
  #Convert node XY to spatial data frame
  transit.spdf_bus <- SpatialPointsDataFrame(coords = tsig_result_bus[,c(3,2)], data = tsig_result_bus,
                                             proj4string = CRS("+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0"))
  
  transit.spdf_rail <- SpatialPointsDataFrame(coords = tsig_result_rail[,c(3,2)], data = tsig_result_rail,
                                              proj4string = CRS("+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0"))

  #transform to planar coordinates
  transit.spdf_rail.proj <- spTransform( transit.spdf_rail, CRS( "+init=epsg:3347" ) ) 
  transit.spdf_bus.proj <- spTransform( transit.spdf_bus, CRS( "+init=epsg:3347" ) ) 
  
  origins.proj <- spTransform( origins[i,], CRS( "+init=epsg:3347" ) ) 

#get closest rail location
near_rail<-gDistance(origins.proj, spgeom2=transit.spdf_rail.proj, byid=T, hausdorff=FALSE, densifyFrac = NULL)
  rail_dist<-min(near_rail)
  rail_xy<-unique(coordinates(transit.spdf_rail[which(near_rail==min(near_rail)),]))
  rail_row<-which(near_rail==min(near_rail))[1]

#get closest bus location
near_bus<-gDistance(origins.proj, spgeom2=transit.spdf_bus.proj, byid=T, hausdorff=FALSE, densifyFrac = NULL)
  bus_dist<-min(near_bus)
  bus_xy<-unique(coordinates(transit.spdf_bus[which(near_bus==min(near_bus)),]))
  bus_row<-which(near_bus==min(near_bus))[1]
  
  ##
  ##get ORIGIN OSM NODE
  ##

  ##Find closest OSM node or the Origin
  select<-"SELECT 
  osm_id,
  ST_Y(ST_Transform(geom_vertex,4326)) AS lat, 
  ST_X(ST_Transform(geom_vertex,4326)) AS lon 
  FROM at_2po_vertex
  WHERE  
  geom_vertex && 
  ST_MakeEnvelope("
  
  bb2<-paste(bbl[1],",",bbl[2],",",bbh[1],",",bbh[2],",","4283")
  q_tsig <- paste(select,bb2,")")
  
  #Pull SQL result
  tsig_result <- dbGetQuery(con_rt, q_tsig)
  
  #Convert node XY to spatial data frame
  node.spdf <- SpatialPointsDataFrame(coords = tsig_result[,c(3,2)], data = tsig_result,
                                      proj4string = CRS("+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0"))
  #transform to planor coordinates
  node.spdf.proj <- spTransform( node.spdf, CRS( "+init=epsg:3347" ) ) 

  #get closest osm node
  near_node<-gDistance(origins.proj, spgeom2=node.spdf.proj, byid=T, hausdorff=FALSE, densifyFrac = NULL)
    node_dist<-min(near_node)
    node_id<-node.spdf$osm_id[which(near_node==min(near_node))]
    
  #get the 'source' node for routing
    select<-"SELECT source
    FROM at_2po_4pgr
    WHERE osm_source_id =" 
    q_node_origin <- paste(select,node_id)
    
    qnode_result_origin <- dbGetQuery(con_rt, q_node_origin)[1,]
    
  #----------------------
    
    ##
    ##get RAIL OSM NODE
    ##
    
    bbl<-rail_xy*.99995  
    bbh<-rail_xy*1.00005  
    
    ##Find closest OSM node
    select<-"SELECT 
    osm_id,
    ST_Y(ST_Transform(geom_vertex,4326)) AS lat, 
    ST_X(ST_Transform(geom_vertex,4326)) AS lon 
    FROM at_2po_vertex
    WHERE  
    geom_vertex && 
    ST_MakeEnvelope("
    
    bb2<-paste(bbl[1],",",bbl[2],",",bbh[1],",",bbh[2],",","4283")
    q_tsig <- paste(select,bb2,")")
    
    #Pull SQL result
    tsig_result <- dbGetQuery(con_rt, q_tsig)
    
    #Convert node XY to spatial data frame
    node.spdf <- SpatialPointsDataFrame(coords = tsig_result[,c(3,2)], data = tsig_result,
                                        proj4string = CRS("+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0"))
    #transform to planor coordinates
    node.spdf.proj <- spTransform( node.spdf, CRS( "+init=epsg:3347" ) ) 
    
    #get closest osm node
    near_node_rail<-gDistance(transit.spdf_rail.proj[rail_row,], spgeom2=node.spdf.proj, byid=T, hausdorff=FALSE, densifyFrac = NULL)
    node_rail_dist<-min(near_node_rail)
    node_rail_id<-node.spdf$osm_id[which(near_node_rail==min(near_node_rail))]
    
    #get the 'source' node for routing
    select<-"SELECT target
    FROM at_2po_4pgr
    WHERE osm_target_id =" 
    q_node_rail <- paste(select,node_rail_id)
    
    qnode_result_rail <- dbGetQuery(con_rt, q_node_rail)[1,]
    
  #----------------------
    
    ##
    ##get bus OSM NODE
    ##
    
    bbl<-bus_xy*.99995  
    bbh<-bus_xy*1.00005  
    
    ##Find closest OSM node
    select<-"SELECT 
    osm_id,
    ST_Y(ST_Transform(geom_vertex,4326)) AS lat, 
    ST_X(ST_Transform(geom_vertex,4326)) AS lon 
    FROM at_2po_vertex
    WHERE  
    geom_vertex && 
    ST_MakeEnvelope("
    
    bb2<-paste(bbl[1],",",bbl[2],",",bbh[1],",",bbh[2],",","4283")
    q_tsig <- paste(select,bb2,")")
    
    #Pull SQL result
    tsig_result <- dbGetQuery(con_rt, q_tsig)
    
    #Convert node XY to spatial data frame
    node.spdf <- SpatialPointsDataFrame(coords = tsig_result[,c(3,2)], data = tsig_result,
                                        proj4string = CRS("+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0"))
    #transform to planor coordinates
    node.spdf.proj <- spTransform( node.spdf, CRS( "+init=epsg:3347" ) ) 
    
    #get closest osm node
    near_node_bus<-gDistance(transit.spdf_bus.proj[bus_row,], spgeom2=node.spdf.proj, byid=T, hausdorff=FALSE, densifyFrac = NULL)
    node_bus_dist<-min(near_node_bus)
    node_bus_id<-tsig_result$osm_id[which(near_node_bus==min(near_node_bus))]
    
    #get the 'source' node for routing
    select<-"SELECT target
    FROM at_2po_4pgr
    WHERE osm_source_id =" 
    q_node_bus <- paste(select,node_rail_id)
    
    qnode_result_bus <- dbGetQuery(con_rt, q_node_bus)[1,]
    
  #----------------------------  
    
      ##get OD dist to rail
        
        q1<-   "SELECT seq, id1 AS node, id2 AS edge,km AS cost
        FROM pgr_astar('
        SELECT id AS id,
        source,
        target,
        cost,
        x1, y1, x2, y2
        FROM at_2po_4pgr as r,
        (SELECT ST_Expand(ST_Extent(geom_way),0.01) as box  FROM at_2po_4pgr as l1  
        WHERE l1.source ="
        o_node<-qnode_result_origin
        q2<-"OR l1.target =" 
        d_node<-qnode_result_rail
        q3<-") as box
        WHERE r.geom_way && box.box',"
        q4<-","
        q5<-", false, false)as r INNER JOIN at_2po_4pgr as g ON r.id2 = g.id ;"
        
        q_railD <- paste0(q1,o_node,q2,d_node,q3,o_node,q4,d_node,q5)
        
        d_result_rail <- dbGetQuery(con_rt, q_railD)
        
        RailDIS_n=sum(d_result_rail$cost)*0.621371
        
  #----------------------------  
    
        ##get OD dist to Bus
        
        q1<-   "SELECT seq, id1 AS node, id2 AS edge,km AS cost
        FROM pgr_astar('
        SELECT id AS id,
        source,
        target,
        cost,
        x1, y1, x2, y2
        FROM at_2po_4pgr as r,
        (SELECT ST_Expand(ST_Extent(geom_way),0.01) as box  FROM at_2po_4pgr as l1  
        WHERE l1.source ="
        o_node<-qnode_result_origin
        q2<-"OR l1.target =" 
        d_node<-qnode_result_bus
        q3<-") as box
        WHERE r.geom_way && box.box',"
        q4<-","
        q5<-", false, false)as r INNER JOIN at_2po_4pgr as g ON r.id2 = g.id ;"
        
        q_busD <- paste0(q1,o_node,q2,d_node,q3,o_node,q4,d_node,q5)
        
        d_result_bus <- dbGetQuery(con_rt, q_busD)
        
        BusDIS_n=sum(d_result_bus$cost)*0.621371
        
  #----------------------------  

dbDisconnect(con)
dbDisconnect(con_rt)
 
#Merge all location results
bs_transit_distance<-as.data.frame(cbind(originID,O_xy,RailDIS_n,rail_xy,BusDIS_n,bus_xy))

}  
stopCluster(cl)
  
})
print(timeTR)
  
write.table(TRANSIT_FINAL, paste0(csvoutname,".csv"), row.names = F, col.names = T, append = F, sep=",",quote=F)



