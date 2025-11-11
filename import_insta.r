# Aus 4CAT exportierte CSV-Datei einlesen ####

library(dplyr)

insta1 <- read_csv("INSERT_FILE_NAME_HERE") # Namen der entsprechenden Datei (inkl. Dateiendung) einfügen

names(insta1)

glimpse(insta1)

# ndjson-Datei aus zeeschuimer einlesen (2 Varianten) ####

library(jsonlite)

insta2 <- stream_in(file("INSERT_FILE_NAME_HERE"))

library(ndjson)

data_list <- ndjson::stream_in("INSERT_FILE_NAME_HERE") # Namen der entsprechenden Datei (inkl. Dateiendung) einfügen

flat_tibble <- dplyr::bind_rows(data_list)

names(flat_tibble)

glimpse(flat_tibble)

# Experimentelle Parsing-Funktion für Instagram-Daten aus zeeschuimer im ndjson-Format ####

source("parse_insta.R") # Skript muss sich im Arbeitsverzeichnis befinden

insta_parsed <- parse_instagram_posts("INSERT_FILE_NAME_HERE") # Namen der entsprechenden Datei (inkl. Dateiendung) einfügen

names(insta_parsed)

glimpse(insta_parsed)

write_csv(parsed_insta_posts, "INSERT_FILE_NAME_HERE") # Namen der Datei (inkl. Dateiendung) einfügen
