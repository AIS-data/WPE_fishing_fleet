library(dplyr)
library(shiny)
library(leaflet)
library(leaflet.extras)

ship <- read.csv("//Locations//20160107.csv", head=FALSE, skip=1)

names(ship) <- c("mmsi", "long", "lat", "V4", "V5", "V6", "V7", "V8", "V9")


ship <- filter(ship, long>=2)
ship <- filter(ship, long<=8)
ship <- filter(ship, lat>=50)
ship <- filter(ship, lat<=55)

ship$long <- round(ship$long, digits=2)
ship$lat <- round(ship$lat, digits=2)

ship <- distinct(ship, long, lat, .keep_all = TRUE)

leaflet(ship) %>%
  addProviderTiles(providers$OpenStreetMap, options = providerTileOptions(noWrap = TRUE)) %>%
  addWebGLHeatmap(lng=~long, lat=~lat, size = 6000)
