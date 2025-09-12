# Bug Fix: Edit pivot_zooplankton to not lose time of day information
library(targets)
library(tidyverse)

# Inputs:
tar_load(zp_abund)
tar_load(zp_key)


zoo_abund <- zp_abund
zoo_key <- zp_key


# # Function to Edit:
 

#' #' @title Pivot Zooplankton Abundances to Long-Format
#' #' 
#' #' @description Takes columns on abundance of zooplankton and converts them into two columns,
#' #' abundance and taxon.
#' #'
#' #' @param zoo_abund 
#' #' @param zoo_key 
#' #'
#' #' @return
#' #' @export
#' #'
#' #' @examples
#' pivot_zooplankton <- function(zoo_abund, zoo_key){
#'   
  # Pivot longer
  zoo_long <- pivot_longer(
    zoo_abund, 
    names_to = "taxon name", 
    values_to = "abundance", 
    cols = 11:ncol(zoo_abund))
  
  # Split taxon name and stage information into separate columns
  zoo_split <- zoo_long %>% 
    separate(
    `taxon name`, 
    into = c("taxa", "stage"), 
    sep = "[_]", 
    fill = "right", 
    remove = FALSE) %>% 
    mutate(taxa = str_to_sentence(taxa),
           stage = tolower(stage))
  
  
  # Join marmap codes in, prep the data
  zoo_erd <- zoo_split %>% 
    left_join(zoo_key, by = c("taxa", "stage")) 
  
  
  # # Check that the stage matched up
  # zoo_erd %>% distinct(`taxon name`, taxa, stage)

    
   # Adjust code to retain hours and minutes 
  zoo_erd_clean <- zoo_erd %>% 
    mutate(
      # Change here
      # date = as.POSIXct(str_c(year, "-", month, "-", day, " ", hour, ":", minute, ":", "00")),
      time = as.Date(str_c(year, "-", month, "-", day)) + hours(hour) + minutes(minute),
      stage = str_trim(stage)) %>% 
    select(
      cruise, 
      transect_number = station, 
      time, # Don't need to rename if called time initially
      latitude        = `latitude (degrees)`, 
      longitude       = `longitude (degrees)`, 
      pci             = `phytoplankton color index`,
      taxon           = taxa,
      taxon_stage     = stage,
      marmap_code     = `marmap taxonomic code:`,
      marmap_stage    = `marmap stage code:`,
      abundance       = abundance)
  

  
  
# }

# What do we have for time of day?
# What is even going on, why can I not combine date and time columns?? It zeros out the hours
# graphing_dt_str is the string I want to parse, graphing_datetime is wrong
dt_plotting <- zoo_erd_clean %>% 
  distinct(time, pci) %>% 
  mutate(
    tod = hms::parse_hms(str_sub(time, -8, -1)),
    #tod = if_else(tod == "00:00:00", hms::parse_hms("00:00:01"), tod),
    flat_dt = as.POSIXct(str_c("2000-01-01 ", tod)), # This works
    graphing_date = as.Date("2021-10-20"), 
    graphing_dt_str = paste(graphing_date, tod),
    graphing_datetime = as.POSIXct(graphing_dt_str)) # this does not...


# Why is it parsing so strangely/incorrectly for graphing_datetime?
dt_plotting %>% glimpse()
 
# Plots wrong too 
ggplot(dt_plotting, aes(time, flat_dt)) +
  geom_point() +
  scale_y_datetime(date_breaks= "2 hours", date_labels = "%H:%M") +
  labs(y = "Time of Day")

# If we plot the hour we can see that its 5AM or 5PM correctly
zoo_abund %>% ggplot(aes(year, hour)) + geom_point()
zoo_abund %>% filter(year %in% c(1973:1980)) %>% distinct(year, hour)

