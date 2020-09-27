library(tmap)
library(tmaptools)
library(sp)
library(dplyr)
library(shiny)
library(leaflet)
library(dplyr)
library(leaflet.extras)

ship1 <- read.csv("//Locations//20160107.csv", head=TRUE)

ship1 <- filter(ship1, mmsi=='xxxxxxxxx')


ship1 <- filter(ship1, lon>=4.00)
ship1 <- filter(ship1, lon<=6.00)
ship1 <- filter(ship1, lat>=50.00)
ship1 <- filter(ship1, lat<=55.00)

ship1$lon <- round(ship1$lon, digits=2)
ship1$lat <- round(ship1$lat, digits=2)

ship1 <- distinct(ship1, lon, lat, .keep_all = TRUE)

leaflet(ship1) %>%
  addProviderTiles(providers$OpenStreetMap) %>%
  addWebGLHeatmap(size=2,units='px') %>%
  addCircleMarkers(~lon, ~lat, popup = ~as.character(mmsi), radius = 1)