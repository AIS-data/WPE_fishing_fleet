# Developer's and user's guide: Implementation of a process of traffic and activity of fishing fleet based on AIS data

## Introduction
This guide is a deliverable of the WPE Tracking ships which is part of the ESSnet Big Data II.

## Overview of process
Our general approach to developing experimental statistics on the behaviour of fishing vessels with the use of AIS data and the fishing fleet register is presented in the scheme below

<p align="center">
  <img src="./img/diagram_process.png" alt="Schematic diagram – process of traffic and activity related for the maritime fishing fleet based on AIS data" width="535">
</p>

## Overview of technology
One of the motivations in the implementation of use-case for fishing fleets was the full use of Big Data methods and technologies, therefore optimal solutions were selected for individual sub-processes (programming languages, libraries, tools, see diagram above). 

For the sub-processes of filtering, cleaning and processing data (responsible for generating statistics and preparing data structures for analyzes), 
we have developed algorithms in **Scala** (processed by **Apache Spark / Hadoop distributed environment**).

For the sub-processes of data visualization and analysis (including traffic intensity), we have developed **R** language codes with library for interactive maps **Leaflet**. Additionally, we have used [movingpandas](https://anitagraser.github.io/movingpandas) libraries. It is a Python library for handling movement data based on Pandas and GeoPandas. The implementation of this library allowed us to generate the traffic trajectory of the analyzed fishing fleet. 

## Data collection and preparation of a reference of frame for fishing fleet
The first step in implementing the process is to download the fishing fleet register for your country.
The fleet register, available from [website of the European Commission](http://ec.europa.eu/fisheries/fleet/index.cfm?lg=en)
and constitutes the basis to receive data on the Polish fishing fleet as of December 31st, in a reference year. 
Every EU country is obliged to report, maintain and update its country’s fishing fleet register in a unified format. 
The fishing fleet register contains a lot of valuable information, e.g.
- Country/state of registration
- Port/place of registration
- Fishing licence
- Ship’s name
- Call sign 
- Overall length
- Engine power


Based on the register, we are able to prepare a reference of frame for fishing fleet which is helpful for data filtering and limiting the size of the data set.

Each ship, regardless of type, in the AIS data is identified by the mmsi number. 
It is worth noting that all fishing vessels are required to use an AIS transmitter above 15 meters, but for safety reasons most smaller fishing vessels also use the AIS system.

Unfortunately, in many cases this number is missing in the registers.
To update/complete information that was missing, we implemented Python web scraping methods to selected tracking vessel website containing ship data. 
Information from web scraping was compared to the Register data and fishing fleet dataset was updated (see diagram below).

<p align="center">
  <img src="./img/diagram_update_register.png" alt="Schematic diagram – supplementing incorrect/missing information via web scraping methods" width="442">
</p>

After supplementing incorrect/missing information, we can create a reference frame of Polish fishing fleets (“Register”) and were able to filter AIS data (“data filtering”), thus limiting our AIS dataset. We simply removed all information from other ships. 

Respecting the business activity of the providers of tracking vessel website solutions, we don't provide a specific solution, but only an example code - **URLscrape_example.py** that can be used to build your own code. Please note that the structure of individual pages may change over time. Of course, with a small amount of the fishing fleet, this process can be done manually.

In addition, when verifying the register of the fishing fleet, we can additionally use AIS static data (id 5) by filtering the vessel type (number 30 means fishing fleet) and selecting the country origin based on the first three characters of the mmsi number. Details in the code at **FF_reference_frame.scala**


## Data processing
<p align="center">
  <img src="./img/diagram_processing_data.png" alt="Schematic diagram – General flow of the algorithm for data processing">
</p>
## Data visualization
