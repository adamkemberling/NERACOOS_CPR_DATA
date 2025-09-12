####  CPR Pipeline Functions
# How to get from raw data to analysis sets
# pipeline tracked using {targets}


####  Packages  ####
options(tidyverse.quiet = T)
suppressWarnings(library(readxl))
suppressPackageStartupMessages(library(tidyverse))



####  Constants  ####


# # Base path to the CPR Data
# gom_cpr_path <- box_path("climate change ecology lab", "Data/Gulf of Maine CPR")
# gom_cpr_path <- paste0(here::here("data_raw"), "/")





####___________________________####
####__  NOAA Gulf of Maine Data  __####

# The column Key lets us make changes to more easily, more robust to future changes
# Easier to look up what columns went into the aggregation this way
# though its a lot of vertical text here



#### 1. Import Raw Data from NOAA  ####




#' @title Separate the CPR Measurement Scales
#' 
#' @description Separates data from the excel workbook sent from NOAA based on 
#' the different measurement scales of the survey. For the data obtained from NOAA
#' the 2 Zooplankton groups from the survey (eyecount and traverse scales) have been combined.
#'
#' @param raw_file Path to excel file that contains sheets for the abundance data
#' @param sample_type zoo or phyto command to indicate which data to pull from excel file
#'
#' @return
#' @export
#'
#' @examples
separate_measure_scales <- function(raw_file, sample_type = c("phyto", "zoo")){
  
  # Excel sheet number changes between zooplankton and zooplankton
  sheet_number <- switch(sample_type,
    "phyto" = 1,
    "zoo"   = 2)
  
  # There is also a differing number of rows in the header
  header_skip <- switch (sample_type,
                         "phyto" = 2,
                         "zoo"   = 2)
  
  # Should get some warnings about duplicate column headers,
  # these get repaired later
  noaa_cpr_raw <- readxl::read_xlsx(
    raw_file,
    skip = header_skip,
    sheet = sheet_number)
    
  return(noaa_cpr_raw)
}





####  2. Repair MARMAP Keys  ####

# Handling duplicated marmap codes, as a discrete step



#' @title Clean Phytoplankton Header Information
#'
#' @description Creates a coherent dataframe where multiple rows of header information do not exist 
#'  prior to the data on abundances of phytoplankton taxa.
#'  
#'  Takes the first two rows of the excel sheet that details information on
#'  what the phytoplankton identity and marmap code is for that taxa, and separates it from the
#'  rest of the data on cruise information and abundances. These ID's are checked for odd-characters
#'  before joining back to the abundance information.
#'
#' @param phyto_raw 
#' @param return_option 
#'
#' @return
#' @export
#'
#' @examples
pull_phyto_pieces <- function(phyto_raw, return_option = c("abundances", "key")){
  
  
  # Number of rows dedicated to the header
  header_rows <- 1
  
  # Number of columns with metadata, before abundance columns
  metadata_cols <- 10
  
  #Pull data out from under the header
  noaa_phyto_abundances <- phyto_raw[(header_rows + 1):nrow(phyto_raw), ]
  
  #Fixing the column names using the header:
  
  # Empty vector starting after the metadata columns:
  phyto_group_stage_names <- rep(NA, ncol(phyto_raw) - metadata_cols)
  
  # For each column after metadata
  for (i in 1:(ncol(phyto_raw) - metadata_cols)) {
    phyto_group_stage_names[i] <- names(phyto_raw[i + metadata_cols])                       #Just the names of the taxa
    phyto_group_stage_names[i] <- str_remove_all(phyto_group_stage_names[i], "'")           #Drop 's
    phyto_group_stage_names[i] <- str_remove_all(phyto_group_stage_names[i], " \\d")        #Drop first digit
    phyto_group_stage_names[i] <- str_remove_all(phyto_group_stage_names[i], "\\d")         #Drop second digit
    phyto_group_stage_names[i] <- str_remove_all(phyto_group_stage_names[i], "\\d")         #Drop third digit
    phyto_group_stage_names[i] <- str_remove_all(phyto_group_stage_names[i], "\\.")         #Drop first period
    phyto_group_stage_names[i] <- str_remove_all(phyto_group_stage_names[i], "\\.")         #Drop second period
    phyto_group_stage_names[i] <- str_remove_all(phyto_group_stage_names[i], "\\.")         #Drop third period
    phyto_group_stage_names[i] <- str_replace_all(phyto_group_stage_names[i], " _", "_")    #Drop space before _
  }
  
  
  #Replace old names with correct names
  names(noaa_phyto_abundances)[(metadata_cols + 1):ncol(noaa_phyto_abundances)] <- phyto_group_stage_names
  
  # Remove the ' from the names 
  names(noaa_phyto_abundances) <- str_replace_all(names(noaa_phyto_abundances), "'", "")
  
  
  ####  Repair MARMAP Key  
  # Prepare header, and make taxon key
  # MARMAP code key: https://www.nefsc.noaa.gov/nefsc/Narragansett/taxcodesA.html
  
  
  # Pull columns referencing taxa
  noaa_phyto_header <- phyto_raw[1, metadata_cols:ncol(phyto_raw)]
  
  # Swap in the formatted names
  names(noaa_phyto_header) <- c("code_type", phyto_group_stage_names)
  
  
  # Make Taxon Key for the marmap values
  noaa_phyto_key <- noaa_phyto_header %>% 
    pivot_longer(cols = -code_type, names_to = "Taxon Name", values_to = "Accepted ID") %>% 
    arrange(`Taxon Name`)
  
  
  # Check for duplicates before reshaping:
  noaa_phyto_dups <- noaa_phyto_key %>% 
    pivot_wider(names_from = code_type, values_from = `Accepted ID`, values_fn = length) %>% 
    filter(`Marmap Code:` > 1)
  
  
  # Address duplicate marmap codes through manually looking them up:
  colnames(noaa_phyto_abundances)[which(phyto_raw[1,] == 9101)] <- "Trichodesmium"
  colnames(noaa_phyto_abundances)[which(phyto_raw[1,] == 9169)] <- "Ceratium ranipes"
  
  
  # Replace accepted ID with marmap code once duplicates have been resolved
  noaa_phyto_key <- noaa_phyto_key %>% 
    mutate(
      `Taxon Name` = ifelse( `Accepted ID` == 9101, "Trichodesmium", `Taxon Name`),
      `Taxon Name` = ifelse( `Accepted ID` == 9169, "Ceratium ranipes", `Taxon Name`)) 
  
  # Can now reshape and then change all names to lowercase
  noaa_phyto_key <- noaa_phyto_key %>%  
    pivot_wider(names_from = code_type, values_from = `Accepted ID`) %>% 
    rename_all(tolower)
  
  
  ####  Prepare them for Export  
  noaa_phyto_abundances <- noaa_phyto_abundances %>%  
    rename_all(tolower)  %>% 
    mutate(cruise = str_replace_all(cruise, "'", ""))
  
  
  # Set an option to return the key or the abundances
  out_options <- list(
    "abundances" = noaa_phyto_abundances,
    "duplicates" = noaa_phyto_dups,
    "key"        = noaa_phyto_key 
  )
  
  # Return either the abundances or the repaired key
  return(out_options[[return_option]])
  
}




#### Zooplankton Marmap Key repair:


#' @title Clean Zooplankton Header Information
#' 
#' @description Creates a coherent dataframe where multiple rows of header information do not exist 
#'  prior to the data on abundances of zooplankton taxa.
#'  
#'  Takes the first three rows of the excel sheet that details information on
#'  what the zooplankton identity and marmap code is for that taxa, and separates it from the
#'  rest of the data on cruise information and abundances. These ID's are checked for odd-characters
#'  before joining back to the abundance information.
#'
#' @param noaa_zoo 
#' @param return_option 
#'
#' @return
#' @export
#'
#' @examples
pull_zoo_pieces <- function(noaa_zoo, return_option = c("abundances", "key")){
  
  
  # Number of rows dedicated to the header
  header_rows <- 3
  
  # number of metadata columns before the header starts
  metadata_cols <- 10
  
  
  #Pull data out from under the header
  noaa_zoo_abundances <- noaa_zoo[(header_rows + 1):nrow(noaa_zoo),]
  
  #Fix names, there's 10 columns before we get to the zooplankton groups and stages
  group_stage_names <- rep(NA, ncol(noaa_zoo) - metadata_cols)
  
  # For each of the names take out any weird characters or digits that read in weird
  for (i in 1:(ncol(noaa_zoo) - metadata_cols)) {
    group_stage_names[i] <- str_c(names(noaa_zoo[i + metadata_cols]), noaa_zoo[1, i + metadata_cols], sep = "_")
    group_stage_names[i] <- str_remove_all(group_stage_names[i], "'")           #Drop 's
    group_stage_names[i] <- str_remove_all(group_stage_names[i], " \\d")        #Drop first digit
    group_stage_names[i] <- str_remove_all(group_stage_names[i], "\\d")         #Drop second digit
    group_stage_names[i] <- str_remove_all(group_stage_names[i], "\\d")         #Drop third digit
    group_stage_names[i] <- str_remove_all(group_stage_names[i], "\\.")         #Drop first period
    group_stage_names[i] <- str_remove_all(group_stage_names[i], "\\.")         #Drop second period
    group_stage_names[i] <- str_remove_all(group_stage_names[i], "\\.")         #Drop third period
    group_stage_names[i] <- str_replace_all(group_stage_names[i], " _", "_")    #Drop space before _
  }
  
  #Replace old names with correct names
  names(noaa_zoo_abundances)[(metadata_cols + 1):ncol(noaa_zoo_abundances)] <- group_stage_names
  names(noaa_zoo_abundances) <- str_replace_all(names(noaa_zoo_abundances), "'", "")
  
  #Fix header, and make taxon key
  noaa_zoo_header <- noaa_zoo[2:3, metadata_cols:ncol(noaa_zoo)]
  names(noaa_zoo_header) <- c("code_type", group_stage_names)
  
  #Reshape Taxon Key to show taxa and stage with marmap codes
  noaa_zoo_key <- noaa_zoo_header %>% 
    pivot_longer(cols = -code_type, 
                 names_to = "Taxon Name", 
                 values_to = "Accepted ID") %>% 
    arrange(`Taxon Name`)
  
  # Check duplicates, should all be unique
  key_dups <- noaa_zoo_key %>% 
    pivot_wider(names_from = code_type, values_from = `Accepted ID`, values_fn = length) %>% 
    filter(`Marmap Taxonomic Code:` > 1 | `Marmap Stage Code:` > 1 ) %>% 
    pull(`Taxon Name`)
  
  
  
  # What duplicate Marmap numbers are representing currently, should be unique to taxa
  zoo_duplicates <- noaa_zoo_key %>% filter(`Taxon Name` %in% key_dups) %>% distinct()
  
  #### Replace taxon names using NEFSC marmap key
  
  # # Which we need to change, manually checked*
  colnames(noaa_zoo_abundances)[which(noaa_zoo[2,] == 3300)] <- "Heteropoda"
  colnames(noaa_zoo_abundances)[which(noaa_zoo[2,] == 4039)] <- "No record"
  
  #Drop column that has no taxa match for the marmap code 
  noaa_zoo_abundances[,"No record"] <- NULL
  
  
  ####  Repair MARMAP Key 
  
  # Make changes to key as well so there is no duplication of codes
  noaa_zoo_key <- noaa_zoo_key %>% 
    mutate(
      `Taxon Name` = ifelse(`Accepted ID` == 3300, "Heteropoda", `Taxon Name`),
      `Taxon Name` = ifelse(`Accepted ID` == 4039, "No record", `Taxon Name`)) %>% 
    distinct() %>% 
    pivot_wider(names_from = code_type, values_from = `Accepted ID`) %>% 
    rename_all(tolower)%>% 
    filter(`taxon name` != "No record")
  
  
  
  # #Separate taxa and stage using regex
  # noaa_zoo_key <- noaa_zoo_key %>% 
  #   mutate(
  #     taxa = str_extract(`taxon name`, "[^_]*"), # Text before underscore
  #     stage = str_extract(`taxon name`, "_.*"),  # Text following underscore
  #     stage = str_replace(stage, "_", "")        # Drop underscore
  # )
  
  # Separate taxa and stage:
  noaa_zoo_key <- noaa_zoo_key %>% 
    separate(`taxon name`,  
             into = c("taxa", "stage"), 
             sep = "[_]", 
             fill = "right", 
             remove = TRUE) %>% 
    mutate(taxa = str_to_sentence(taxa),
           stage = tolower(stage))
  
  
  ####  Prep for Export 
  noaa_zoo_abundances <- noaa_zoo_abundances %>%  
    rename_all(tolower) %>% 
    mutate(cruise = str_replace_all(cruise, "'", ""))
  
  
  # Put the key and the abundances in a list for exporting either
  out_options <- list(
    "abundances" = noaa_zoo_abundances,
    "duplicates" = zoo_duplicates,
    "key"        = noaa_zoo_key 
  )
  
  return(out_options[[return_option]])
  
}




####  3. Reshape Abundance Data  ####

# Pivot the table into longer format


#' @title Pivot Phytoplankton Abundances to Long-Format
#' 
#' @description Takes columns on abundance of phytoplankton and converts them into two columns,
#' abundance and taxon.
#'
#' @param phyto_abund 
#' @param phyto_key 
#'
#' @return
#' @export
#'
#' @examples
pivot_phyto <- function(phyto_abund, phyto_key){
  
  # Pivot the abundances into a long format
  phyto_long <- pivot_longer(phyto_abund, 
                             names_to = "taxon name", 
                             values_to = "abundance", 
                             cols = 11:ncol(phyto_abund))
  
  
  # Join the marmap codes in & prep/format the data
  phyto_erd <- phyto_long %>% 
    mutate(
      time = as.Date(str_c(year, "-", month, "-", day)) + hours(hour) + minutes(minute),
      `taxon name` = str_to_sentence(`taxon name`)) %>% 
    left_join(phyto_key, by = "taxon name") 
  
  # clean up columns
  phyto_erd_clean <- phyto_erd %>% 
    select(cruise, 
           transect_number = station, 
           time,
           latitude = `latitude (degrees)`, 
           longitude = `longitude (degrees)`, 
           pci = `phytoplankton color index`,
           taxon = `taxon name`,
           marmap_code = `marmap code:`,
           abundance = abundance)
  
  
  return(phyto_erd_clean)
}










#' @title Pivot Zooplankton Abundances to Long-Format
#' 
#' @description Takes columns on abundance of zooplankton and converts them into two columns,
#' abundance and taxon.
#'
#' @param zoo_abund 
#' @param zoo_key 
#'
#' @return
#' @export
#'
#' @examples
pivot_zooplankton <- function(zoo_abund, zoo_key){
  
  # Pivot longer
  zoo_long <- pivot_longer(
    zoo_abund, 
    names_to = "taxon name", 
    values_to = "abundance", 
    cols = 11:ncol(zoo_abund))
  
  # Split taxon name and stage
  zoo_split <- zoo_long %>% 
    separate(`taxon name`, 
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
  
  # reformat columns
  zoo_erd_clean <- zoo_erd %>% 
    mutate(
      # date = as.POSIXct(str_c(year, "-", month, "-", day, " ", hour, ":", minute, ":", "00")),
      time = as.Date(str_c(year, "-", month, "-", day)) + hours(hour) + minutes(minute),
      stage = str_trim(stage)) %>% 
    select(cruise, 
           transect_number = station, 
           # time = date, 
           time, 
           latitude = `latitude (degrees)`, 
           longitude = `longitude (degrees)`, 
           pci = `phytoplankton color index`,
           taxon = taxa,
           taxon_stage = stage,
           marmap_code = `marmap taxonomic code:`,
           marmap_stage = `marmap stage code:`,
           abundance = abundance)
  
  
}












####___________________________####
####__ MBA GOM Data  __####







#### 1. Import MBA CPR Data  ####
# from Marine Biological Association

#' @title Load MBA CPR Abundance Data
#' 
#' @description Load one of the three measurement scales of the Gulf of Maine
#' CPR program: phytoplankton, traverse, or eyecount
#'
#' @param raw_path Path to the excel file that CPR data is stored on
#' @param sample_type one of: "phyto", "trav", or "eye"
#'
#' @return
#' @export
#'
#' @examples
mba_abundance_dat <- function(raw_path, sample_type){
  
  
  # Paths to the two excel files
  mc1_path <- str_c(raw_path, "MC part1.xlsx")
  mc2_path <- str_c(raw_path, "MC part 2.xlsx")
  
  # excel sheet number to use for abundances:
  abund_sheet_num <- switch(sample_type,
                            "phyto" = 1,
                            "eye"   = 2,
                            "trav"  = 3)
  
  
  # Read the correct sheet of excel files for abundances:
  mc1_abund <- readxl::read_xlsx(mc1_path, sheet = abund_sheet_num)
  mc2_abund <- readxl::read_xlsx(mc2_path, sheet = abund_sheet_num)
  
  # Bind them into one df
  abund_dat <- bind_rows(list(mc1_abund, mc2_abund))
  
  # return abundances
  return(abund_dat)
  
} 





####  2. Build Taxon Key  ####

#' @title Build MBA CPR Taxa Key
#'
#' @description Load the identification key for one of the three measurement scales of the Gulf of Maine
#' CPR program: phytoplankton, traverse, or eyecount
#'
#' @param raw_path Path to the excel file that CPR data is stored on
#' @param sample_type one of: "phyto", "trav", or "eye"
#'
#' @return
#' @export
#'
#' @examples
extract_mba_key <- function(raw_path, sample_type){
  
  
  # Taxa Information:
  
  # Paths to the two excel files
  mc1_path <- str_c(raw_path, "MC part1.xlsx")
  mc2_path <- str_c(raw_path, "MC part 2.xlsx")
  
  # excel sheet number for taxa keys:
  taxa_sheet_num <- switch(sample_type,
                           "phyto" = 5,
                           "eye"   = 7,
                           "trav"  = 6)
  
  # MC2 doesn't have individual sheets for its keys, so we need to specify columns
  mc2_key_cols <- switch(sample_type,
                         "phyto" = c(2,  1,  5),
                         "eye"   = c(14, 13, 16),
                         "trav"  = c(8,  7,  10))
  
  # These are separated onto their own sheets:
  mc1_taxa <- readxl::read_xlsx(mc1_path, sheet = taxa_sheet_num, skip = 1) %>% 
    setNames(c("taxon name", "taxa_id_num", "note"))
  
  # The second group was not
  mc2_taxa <- readxl::read_xlsx(mc2_path, sheet = 4, skip = 1) %>% 
    select(c(mc2_key_cols)) %>% 
    setNames(c("taxon name", "taxa_id_num", "note"))
  
  # Join them
  key_full <- full_join(mc1_taxa, mc2_taxa, by = c("taxon name", "taxa_id_num", "note")) %>% 
    mutate(`taxa_id_num` = as.character(`taxa_id_num`))
  
  
}




#### 3. Reshape Data  ####


#' @title Pivot and format MBA CPR Data for ERDDAP
#'
#' @param abund_dat 
#' @param key_full 
#' @param sample_type 
#'
#' @return
#' @export
#'
#' @examples
pivot_mba_data <- function(abund_dat, key_full, sample_type){
  
  # Identify which columns are the location/time metadata
  # phytoplankton has chlorophyll index so that one has another column
  metadata_cols <- switch(sample_type,
                          "phyto" = 7,
                          "eye"   = 6,
                          "trav"  = 6)
  
  # Pivot longer
  abund_long <- abund_dat %>% 
    pivot_longer(cols = c((metadata_cols+1):ncol(abund_dat)), 
                 names_to = "taxa_id_num", 
                 values_to = "abundance_per_transect")
  
  
  # Join the key in:
  abunds_labelled <- left_join(abund_long, key_full) %>% 
    rename(id_number = `taxa_id_num`) %>% 
    select(-abundance_per_transect, everything())
  
  
  
  # Final formatting steps for ERDDAP:
  abunds_labelled <- abunds_labelled %>% 
    mutate(cruise = str_extract(Sample_Id, "[^-]*"),
           station = str_extract(Sample_Id, "-.*"),
           station = str_replace(station, "-", "")) 
  
  # Set columns to select based on sample_type
  if(sample_type == "phyto"){
    abunds_ready <- abunds_labelled %>% 
      select(cruise,
             transect_number = station,
             time = Midpoint_Date_Local,
             latitude = Latitude,
             longitude = Longitude,
             pci = Chlorophyll_Index,
             taxon = `taxon name`,
             mba_id = id_number, #NOT the same as marmap codes aphia code WoRMS
             abundance_per_transect)
  } else{
    
    # Can't grab PCI if it doesn't exist
    abunds_ready <- abunds_labelled %>% 
      select(cruise,
             transect_number = station,
             time = Midpoint_Date_Local,
             latitude = Latitude,
             longitude = Longitude,
             # pci = Chlorophyll_Index,
             taxon = `taxon name`,
             mba_id = id_number, #NOT the same as marmap codes
             abundance_per_transect)
    
  }
  
  
  
  
  # Return the pivoted data
  return(abunds_ready)
  
  
}














####  4. Unit Conversions  ####


# math for transforming abundance per transect to abundance per 100m3 or 1m3
transect_to_m3 <- function(transect_abundance, out_units = c("meters cubed", "100 meters cubed")){
  

  #---  Unit Conversion Details -
  
  # NOAA Data is in units of 1m3 for phytoplankton and 100m3 for zooplankton
  # This function exists to convert from the CPR abundances of
  # number per transect (1 silk mesh section) to the NOAA units
  
  
  #--- Categorical Counting System
  # The CPR methodology counts abundance for a different amount
  # of the silk sample based on the sizes of the organisms being counted
  # This is done at three scales, counting abundances across the following
  # sub-sample sizes:
  
  # A. The proportions of the silk mesh that is 
  # sub-sampled and extrapolated using a stepped-scale
  # based on bins corresponding to a log-scale
  
  # phyto    1/8000th of transect counted
  # traverse 1/40th of transect counted
  # eyecount Full transect counted
  
  #--- Converting from transect abundance to equivalent water volume
  
  # Distance Traveled (towed) corresponding to one sample
  # 1 transect = 10 nautical miles
  
  # Distance towed in meters
  # 1852 meters in 1 nautical mile, * 10 for full transect
  
  # Edge Dimensions of the CPR opening aperture
  # CPR aperture dimensions = 1.27 cm square entrance
  
  # Area of the CPR device's opening in m2
  # aperture area in square meters = 0.00016129
  
  # Volume of water that is sampled per 10cm silk (length * width * height)
  # 2.987091 meters^3
  
  
  #--- abundance per meters cubed:
  
  # Original calculation to get to # per meters cubed
  conversion_rate <- 1 / 2.987091
  
  # Multiply by 100 to get to 100 cubic meters if needed
  conversion_rate <- switch(
    out_units,
    "meters cubed" = conversion_rate,
    "100 meters cubed" = conversion_rate * 100
  )
  
  
  # Convert units:
  converted_abundance <- transect_abundance * conversion_rate
  return(converted_abundance)
  
  
  
  }


#### 5. Add PCI to Zooplankton  ####

supplement_PCI <- function(phyto_dat, zooplankton_dat){
  
  
  # PCI comes from phytoplankton data
  transect_pci <- distinct(phyto_dat, cruise, transect_number, time, 
                      latitude, longitude, pci)
  
  # Merge station details with PCI into MBA zooplankton information
  mba_zooplankton_pci <- full_join(transect_pci, zooplankton_dat)
  
  # Return the new data
  return(mba_zooplankton_pci)
  
}



####__________________####
####____Export to Box____####




#' @title Export Files to Box, for ERDDAP
#' 
#' @description The last step before the data goes onto ERDDAP, is the csv containing the data and 
#' an XML describing it are used to create a s3 datasource that ERDDAP can reference for the file.
#'
#' @param noaa_phyto NOAA Phytoplankton
#' @param noaa_zoo NOAA Zooplankton
#' @param mba_phyto MBA Phytoplankton
#' @param mba_trav MBA Traverse Zooplankton
#' @param mba_eye MBA Eyecount Zooplankton
#'
#' @return
#' @export
#'
#' @examples
targets_tobox <- function(noaa_phyto, noaa_zoo, mba_phyto, mba_trav, mba_eye){

  # # This function does not seem to overwrite anything... or work, wtf Box
  # plz excuse profanity I have covid
  # # Path to organization's Box drive -
  # box_path <- gmRi::cs_path("res", "CPR_ERDDAP")
  # box_path <- gmRi::cs_path("res", "CPR_ERDDAP")
  # gom_path <- str_c(box_path, "/gulfofmaine_cpr/")
  # dat_path <- str_c(gom_path, "/data/")
  # xml_path <- str_c(gom_path, "/xml/")
  
  
  # Make any last second changes that should happen to all of them:
  
  # Make sure longitude is negative...
  walk(
    .x = list(noaa_phyto, noaa_zoo, mba_phyto, mba_trav, mba_eye),
    .f = function(x){x <- mutate(x, longitude = ifelse(longitude > 0, longitude *-1, longitude))}
  )
  

  # Use here, then manually move:
  here_path <- here::here("erddap_ready")
  
  # Data
  write_csv(noaa_phyto, file = str_c(here_path, "/noaa_gom_cpr_phytoplankton.csv"))
  write_csv(noaa_zoo,   file = str_c(here_path, "/noaa_gom_cpr_zooplankton.csv"))
  write_csv(mba_phyto,  file = str_c(here_path, "/mba_gom_cpr_phytoplankton.csv"))
  write_csv(mba_trav,   file = str_c(here_path, "/mba_gom_cpr_traverse.csv"))
  write_csv(mba_eye,    file = str_c(here_path, "/mba_gom_cpr_eyecount.csv"))


  # XML

}


####________________####
####____  Joining Prep  ####

#### 1. Separate Taxa and Stage  ####
separate_taxastage <- function(z_dat){
  # Words that indicate the stage and not a taxonomic ID
  stage_identifiers <- c(
    "adult",
    "calyptopis",
    "copepodite", 
    "cyphonautes",
    "cypris",
    "cyst",
    "egg", 
    "immature", 
    "i-iv",
    "larva",
    "larvae",
    "medusa",
    "megalopa",
    "molt",
    "nauplii",
    "nauplius",
    "parva",  
    "post",
    "postlarva",
    "(total)",
    "total",
    "trochophore",
    "unstaged", 
    "unidentified",
    "(unidentified)",
    "v-vi",
    "(v-vi)",
    "veliger", 
    "with",
    "without",
    "zoea",
    "NA")
  
  
  # Make a copy, but index the rows:
  # format the taxa to lowercase
  # replace spp. and spp since they are redundant and NOAA data does not have it 
  z_copy <- z_dat %>% 
    mutate(row_i = row_number(),
           taxa_mba = tolower(taxon),
           taxa_mba = str_replace_all(string = taxa_mba, pattern = " spp.", ""),
           taxa_mba = str_replace_all(string = taxa_mba, pattern = " spp", ""),
           taxa_mba = str_replace_all(string = taxa_mba, pattern = " eyecount", "")) 
  
  # Pull out the first word, second word, and third
  # these will be used to check whether the the stage is being described or
  # whether it is still the taxon
  z_taxa <- z_copy %>% 
    transmute(
      row_i = row_number(),
      taxa_mba = taxa_mba,
      word_1 = word(taxa_mba, start = 1, end = 1, sep = " "),
      word_2 = word(taxa_mba, start = 2, end = 2, sep = " "),
      word_3 = word(taxa_mba, start = 3, end = 3, sep = " "))
  
  # If the second word is a stage identifier like copepodite
  # then the 2nd-last word is the taxon stage
  # And the taxon itself is just word_1
  singles <- z_taxa %>% 
    filter( (word_2 %in% stage_identifiers | is.na(word_2)) ) %>% 
    mutate(taxa = word_1,
           taxon_stage = word(taxa_mba, 2, -1, sep = " ")) %>% 
    select(row_i, taxa, taxon_stage)
  
  # If the second word is not part of the stage identifier than it is part of the taxon
  # But now since some are more than 2 words long we have this nonsense
  doubles <- z_taxa %>% 
    filter(
      row_i %nin% singles$row_i,
      ( word_3 %in% stage_identifiers | is.na(word_3) )
    ) %>% 
    mutate(taxa = str_c(word_1, " ", word_2),
           taxon_stage = word(taxa_mba, 3, -1, sep = " ")) %>% 
    select(row_i, taxa, taxon_stage)
  
  # successes
  successes <- c(singles$row_i, doubles$row_i)
  
  
  # # What is left?
  # z_taxa %>% 
  #   filter(row_i %nin% successes) %>% 
  #   distinct(taxa_mba)
  
  # Just these:
  # Fusopsis incertae sedis
  # Thecosomata (north atlantic)
  
  # For whatever is left, the taxon is the only information
  leftovers <- z_taxa %>% 
    filter(row_i %nin% successes) %>% 
    mutate(taxa = taxa_mba,
           taxon_stage = NA) %>% 
    select(row_i, taxa, taxon_stage)
  
  
  
  # Join them up
  taxa_split <- bind_rows(
    list(singles, doubles, leftovers))
  
  # Add to the mba_zooplankton
  # Drop the original one that was long and had both:
  mba_splittaxa <- z_copy %>% 
    select(-taxon) %>% 
    left_join(taxa_split, by = "row_i") 
  
  
  # Tidy up
  mba_splittaxa <- mba_splittaxa %>% 
    mutate(taxa = str_to_sentence(taxa)) %>% 
    select(cruise, transect_number, time, latitude, longitude, pci, 
           taxon = taxa, taxon_stage, mba_id, abundance_per_transect, abundance)
  
  # Return the split taxa
  return(mba_splittaxa)
}


####





####______________________####
####__ Joining Datasets  __####



####  Prepare NOAA and SAHFOS for Join  ####
#' 
#' 
#' consolidate_noaa_taxa <- function(noaa_abundances){
#'   
#'   ####  Cleaner NOAA Consolidation  
#'   
#'   
#'   ####  1. Remove unused Taxa  
#'   #These names are not found at all in our data
#'   no_records <- noaa_abundances %>% 
#'     pivot_longer(names_to = "taxon_stage", values_to = "abundance", 
#'                  cols = 11:ncol(noaa_abundances)) %>% 
#'     mutate(abundance = as.numeric(abundance)) %>% 
#'     group_by(taxon_stage) %>% 
#'     summarise(presence = ifelse(sum(abundance, na.rm = T) > 0, "present", "absent")) %>% 
#'     ungroup() %>% 
#'     filter(presence == "absent") %>% 
#'     pull(taxon_stage)
#'   
#'   
#'   #Take the species with abundances out  of the rest of the data
#'   noaa_inuse <- noaa_abundances %>% select(-one_of(no_records))
#'   
#'   
#'   ####  2. Make Column Key,  prep abundance and metadata
#'   
#'   # Key moved to top level ^  
#'   
#'   # Pull the station information
#'   cpr_metadata <- noaa_abundances[, 1:10]
#'   
#'   # Make sure abundances are numeric for combining them correctly
#'   cpr_abundances <- noaa_abundances[, 11:ncol(noaa_abundances)] %>% 
#'     mutate(across(everything(), as.numeric))
#'   
#'   ####  3. Use  Key to Consolidate Abundances
#'   
#'   # Created in R/targets_support/noaa_taxa_key_creation.R
#'   noaa_gom_taxa_key <- readRDS(file = here::here("data_processed/noaa_gulfofmaine_taxa_key.rds"))
#'   
#'   # Consolidate in use taxa by stepping through groups of the taxa key
#'   # Keys that match a single column are treated as one column
#'   # Groups of multiple taxa stages are added together
#'   noaa_gom_refined <- imap(noaa_gom_taxa_key, function(x, y) {
#'     
#'     # Set the value used to replace -9999
#'     # NOTE -9999 was replaced with 0 for the publication I worked on with Andy
#'     # In hindsight it makes more sense to set them as the lowest value on scale or NA
#'     uncounted_replacement_value <- NA
#'     
#'     
#'     # So for names that have exact matches the transfer is easy
#'     if(length(x) == 1) {
#'       #Pull the correct column
#'       taxa_new <- data.frame(y = select(cpr_abundances, one_of(x)))
#'       # replace -9999
#'       taxa_new[taxa_new == -9999] <-  uncounted_replacement_value
#'       #set name
#'       colnames(taxa_new)[1] <- y
#'     } 
#'     
#'     # For cases where you are consolidating it is a little different:
#'     # need some way to sum the columns by row element, not collapse to sum the column
#'     if(length(x) > 1) {
#'       
#'       # Build dataframe of appropriate dimensions
#'       mult_columns_df <- as.data.frame(matrix(nrow = nrow(cpr_abundances), ncol = length(x) + 1))
#'       
#'       # fill in values from the columns that constitute the group
#'       for (i in 1:length(x)) {
#'         mult_columns_df[,i] <- select(cpr_abundances, one_of(x[i])) }
#'       
#'       # replace -9999
#'       mult_columns_df[mult_columns_df == -9999] <- uncounted_replacement_value
#'       
#'       # Add the columns together to get a sum column
#'       for (i in 1:nrow(mult_columns_df)) {
#'         mult_columns_df[i, length(x) + 1] <- sum(mult_columns_df[i, 1:length(x)], na.rm = T) }
#'       
#'       # Pull the sum column, set the name
#'       taxa_new <- data.frame(y = mult_columns_df[, length(x) + 1])
#'       colnames(taxa_new)[1] <- y
#'     }
#'     
#'     # return the new taxa groups
#'     return(taxa_new)
#'     
#'   }) %>% bind_cols()
#'   
#'   
#'   #Join the new columns back to the station_metadata
#'   noaa_gom_prepared <- bind_cols(cpr_metadata, noaa_gom_refined)
#'   
#'   # format station column to bind later with sahfos
#'   noaa_gom_prepared$station <- as.character(noaa_gom_prepared$station)
#'   
#'   return(noaa_gom_prepared)
#'   
#' }
#' 
#' 
#' 
#' # Original Source: 17_noaa_sahfos_eda.R
#' # match sahfos column names to the noaa taxa groups
#' match_sahfos_to_noaa <- function(sahfos_zoo){
#'   
#'   #Renaming of sahfos data to match the refined noaa list
#'   sahfos_zoo_2 <- sahfos_zoo %>% 
#'     rename(
#'       `acartia spp.`              = `acartia spp. (unidentified)`,
#'       `amphipoda spp.`            = `amphipoda (unidentified)`,
#'       `appendicularia spp.`       =  appendicularia,
#'       `bivalvia spp.`             = `bivalvia larvae`,
#'       `calanus finmarchicus v-vi` = `calanus finmarchicus`,
#'       `calanus i-iv`              = `calanus i-iv`,
#'       `calanus spp.`              = `calanus v-vi unidentified`, 
#'       `centropages spp.`          = `centropages spp. (unidentified)`,
#'       `cumacea spp.`              =  cumacea,
#'       `doliolidae spp.`           =  doliolidae,
#'       `euchaeta spp.`             = `euchaetidae (unidentified)`,
#'       `euphausiacea spp.`         = `euphausiacea total`,
#'       `foraminifera spp.`         = `foraminifera (total)`,
#'       `gammaridea spp.`           =  gammaridea,
#'       `gastropoda spp.`           = `gastropoda (unidentified)`,
#'       `gymnosomoata spp.`         = `gymnosomata (unidentified)`,
#'       `harpacticoida spp.`        = `harpacticoida total traverse`,
#'       `hyperiidea spp.`           = `hyperiidea (total)`,
#'       `ischnocalanus spp.`        =  ischnocalanus,
#'       `lepas spp.`                = `lepas nauplii`,
#'       `metridia spp.`             = `metridia spp. (v-vi) (unidentified)`,
#'       `monstrilloida spp.`        =  monstrilloida,
#'       `ostracoda spp.`            =  ostracoda,
#'       `pleuromamma spp.`          = `pleuromamma spp. (unidentified)`,
#'       `polychaeta larva`          = `polychaete larvae (unidentified)`,
#'       `salpidae spp.`             = `salpidae (total)`,
#'       `siphonostomatoida spp.`    =  siphonostomatoida,
#'       `thecosomata spp.`          = `thecosomata (north atlantic)`,
#'       `tintinnidae spp.`          = `tintinnida total` ) 
#'   
#'   #This section is for when multiple columns need to be reduced to a single aggregate
#'   sahfos_zoo_renamed <- sahfos_zoo_2 %>% 
#'     mutate(
#'       `candacia spp.`                     = `candacia i-iv` + `candacia spp. (unidentified)`, 
#'       `candacia i-iv`                     =  NULL,
#'       `candacia spp. (unidentified)`      =  NULL,
#'       `copepoda spp.`                     = `copepod eggs` + `copepod nauplii`,
#'       `copepod eggs`                      =  NULL,
#'       `copepod nauplii`                   =  NULL,
#'       `decapoda spp.`                     = `decapod megalopa` + `decapod zoea` + `decapoda larvae (total)`,
#'       `decapod megalopa`                  =  NULL, 
#'       `decapod zoea`                      =  NULL,
#'       `decapoda larvae (total)`           =  NULL,
#'       `fish eggs`                         = `fish eggs (total)` + `fish eggs with oil globules` + `fish eggs without oil globules`,
#'       `fish eggs (total)`                 =  NULL,
#'       `fish eggs with oil globules`       =  NULL,
#'       `fish eggs without oil globules`    =  NULL,
#'       `pseudocalanus spp.`                = `pseudocalanus spp. adult atlantic` + `pseudocalanus spp. adult total`,
#'       `pseudocalanus spp. adult atlantic` =  NULL, 
#'       `pseudocalanus spp. adult total`    =  NULL,
#'       `radiolaria spp.`                   = `radiolaria non-acantharian` + `radiolaria total`,
#'       `radiolaria non-acantharian`        =  NULL,
#'       `radiolaria total`                  =  NULL,
#'       sample_id                           =  NULL)
#'   
#'   return(sahfos_zoo_renamed)
#'   
#'   
#'   
#' }
#' 
#' 
#' 
#' 
#' #Bind the two data sources
#' join_zoo_sources <- function(noaa_zoo_refined, sahfos_zoo_renamed){
#'   
#'   combined_set <- bind_rows(
#'     list("NOAA"   = noaa_zoo_refined, 
#'          "SAHFOS" = sahfos_zoo_renamed), 
#'     .id = "Data Source")
#'   
#'   return(combined_set)
#' }
#' 
#' 
#' 
#' ####________________________####
#' ####  Seasonal Spline Prep  ####
#' 
#' cpr_spline_prep <- function(cpr_abundances){
#'   cpr_date_prepped <- cpr_abundances %>% 
#'     mutate(cal_date = as.POSIXct(str_c(year, month, day, sep = "/"), format = "%Y/%m/%d"), .after = "day") %>% 
#'     mutate(jday = lubridate::yday(cal_date), .after = "cal_date") %>% 
#'     rename(lon = `longitude (degrees)`,
#'           latitude = `latitude (degrees)`)
#'   
#'   return(cpr_date_prepped)
#' }
#' 
#' 
#' 
#' 
#' 
#' #### Trim Data to Study Area BBox  ####
#' 
#' 
#' 
#' #' @title CPR Area Filter
#' #' 
#' #' @description Subset the Gulf of Maine CPR survey data using a specific survey area, specified by name. 
#' #' Extents for the following areas are available: 
#' #'
#' #' @param study_area Indication of what area to subset with. Includes GOM, GOM_new, CCB, WGOM, EGOM, SS.
#' #'
#' #' @return 
#' #' @export
#' #'
#' #' @examples
#' cpr_area_crop <- function(cpr_dat, study_area = "GOM_new"){
#'   
#'   area_bboxes <- tribble( ##### Area BBbox Open  ####
#'                           ~"area",  ~"lon",  ~"lat",
#'                           #Gulf of Maine - Historic
#'                           "gom",   -70.000000,	42.200000,
#'                           "gom",   -68.600000,	42.200000,
#'                           "gom",   -68.600000,	42.400000,
#'                           "gom",   -66.600000,	42.400000,
#'                           "gom",   -66.600000,	43.400000,
#'                           "gom",   -68.600000,	43.400000,
#'                           "gom",   -68.600000,	43.200000,
#'                           "gom",   -70.000000,	43.200000,
#'                           "gom",   -70.000000,	42.200000,
#'                           
#'                           #Gulf of Maine - Extended North to capture cpr route change
#'                           "gom_new",   -70.000000,	42.200000, 
#'                           "gom_new",   -66.600000,	42.200000, 
#'                           "gom_new",   -66.600000,	43.800000, 
#'                           "gom_new",   -70.000000,	43.800000, 
#'                           "gom_new",   -70.000000,	42.200000,
#'                           
#'                           "ccb",   -70.800000,	42.200000,
#'                           "ccb",   -70.000000,	42.200000,
#'                           "ccb",   -70.000000,	42.800000,
#'                           "ccb",   -70.800000,	42.800000,
#'                           "ccb",   -70.800000,	42.200000,
#'                           
#'                           #Western Gulf of Maine
#'                           "wgom",  -70.000000, 	42.200000,
#'                           "wgom",  -68.600000, 	42.200000,
#'                           "wgom",  -68.600000, 	43.200000,
#'                           "wgom",  -70.000000, 	43.200000,
#'                           "wgom",  -70.000000, 	42.200000,
#'                           
#'                           #Eastern Gulf of Maine
#'                           "egom",  -68.600000,	42.400000,
#'                           "egom",  -66.600000,	42.400000,
#'                           "egom",  -66.600000,	43.400000,
#'                           "egom",  -68.600000,	43.400000,
#'                           "egom",  -68.600000,	42.400000,
#'                           
#'                           "ss",    -66.600000,	42.600000,
#'                           "ss",    -65.400000,	42.600000,
#'                           "ss",    -65.400000,	43.400000,
#'                           "ss",    -66.600000,	43.400000,
#'                           "ss",    -66.600000,	42.600000,
#'   ) %>% arrange(area) ##### Area BBbox Close  ####
#'   
#'   
#'   #Filter to the correct area
#'   study_area_bbox <- filter(area_bboxes, area == tolower(study_area))
#'   
#'   #subset data that fits
#'   cpr_dat <- cpr_dat %>% mutate(lon = ifelse(lon > 0, lon * -1, lon))
#'   
#'   cpr_dat <- cpr_dat %>% 
#'     filter(
#'       between(lon, min(study_area_bbox$lon), max(study_area_bbox$lon)),
#'       between(lat, min(study_area_bbox$lat), max(study_area_bbox$lat)))
#'   
#'   # Return the cpr data
#'   return(cpr_dat)
#' }
#' 
#' 
#' ####  Spline Tools  ####
#' 
#' 
#' # Split taxa into list, drop groups only present in NOAA or SAHFOS:
#' split_cpr_by_taxa <- function(cpr_spline_prepped){
#'   
#'   # testing: 
#'   # tar_load(gom_area_cropped); cpr_spline_prepped <- gom_area_cropped
#'   
#'   # Start / End Taxa, instead of assuming no columns get added before/after
#'   start_taxa <- which(names(cpr_spline_prepped) == "phytoplankton color index")
#'   end_taxa <- which(names(cpr_spline_prepped) == "radiolaria spp.")
#'   
#'   # Identify the columns that represent abundances
#'   # taxa_cols <- names(cpr_spline_prepped)[12:ncol(cpr_spline_prepped)]
#'   taxa_cols <- names(cpr_spline_prepped)[start_taxa:end_taxa]
#'   names(taxa_cols) <- taxa_cols
#'   
#'   # Make a list with details on each taxa
#'   taxa_list <- map(taxa_cols, function(x){
#'     taxa_name <- sym(x)
#'     taxa_subset <- cpr_spline_prepped %>% 
#'       select(year, jday, lat, lon, abundance = !!taxa_name)
#'   })
#'   
#'   #Find those pesky NA taxa
#'   na_counts <- map(taxa_list, function(x){
#'     sum(is.na(x$abundance))}) %>% 
#'     bind_cols() %>%
#'     pivot_longer(names_to = "taxa", values_to = "total NA's", cols = everything()) 
#'   
#'   # falg taxa that are in either period, or just completely absent
#'   na_counts <- na_counts %>%
#'     mutate(status = case_when(
#'       `total NA's` == 290 ~ "NOAA only",
#'       `total NA's` == 4799 ~ "SAHFOS only",
#'       `total NA's` == 0 ~ "Found in both",
#'       `total NA's` > 4799 ~ "Too Many NA's",
#'       `total NA's` == nrow(na_counts) ~ "drop",
#'       TRUE ~ "unclear"
#'       
#'     ))
#'   
#'   # #Taxa with full time series
#'   # keepers <- filter(na_counts, status == "Found in both")
#'   keepers <- filter(na_counts, status != "drop")
#'   fullts_taxa <- taxa_list[names(taxa_list) %in% keepers$taxa]
#'   
#'   # Return just the taxa that exist in both periods
#'   return(fullts_taxa)
#'   
#' }
#' 
#' 
#' 
#' #' Continuous Plankton Recorder Seasonal Spline Tool
#' #'
#' #' @param cpr_dat Takes output of cpr_area_crop. A data.frame containing the following columns: year, julian day, lat, lon, and abundance.
#' #' @param spline_bins Integer value indicating the desired number of basis functions for the seasonal spline fit, default = 10
#' #' @param season_bins Integer value indicating the number of periods you wish there to be in a recurring 365 day cycle. 
#' #' These are used as labels, not in GAM fitting.
#' #'
#' #' @return  List containing 1. the original dataframe with log transformed abundances, labeled periods, and log transformed anomalies
#' #'  2. the mean, standard deviation, and sample size for each period for each year, and 3. the GAMS themselves
#' #' @export
#' #'
#' #' @examples
#' cpr_spline_fun <- function(cpr_dat = cpr_data, spline_bins = 10, season_bins = 4) {
#'   ####  Fit Seasonal Trend using all data  ####
#'   
#'   
#'   #Check Input dimensions are correct
#'   if(ncol(cpr_dat) != 5) {return(print('dat format requires 5 columns: [year, jday, lat, lon, #]'))}
#'   
#'   #Transform abundance to log abundance
#'   cpr_dat <- cpr_dat %>% 
#'     mutate(
#'       abundance = as.numeric(abundance),
#'       log_abund = log(abundance),
#'       log_abund = ifelse(is.infinite(log_abund), 0, log_abund)
#'     )
#'   
#'   
#'   #Build spline model using mgsv::gam using cyclic penalized cubic regression spline smooth
#'   cc_spline_mod <- gam(log_abund ~  s(jday, bs = "cc", k = spline_bins),
#'                        data = cpr_dat)
#'   
#'   
#'   #Add Predictions back to the data
#'   cpr_dat$spline_pred <- predict(cc_spline_mod, cpr_dat, type = "response")
#'   
#'   
#'   #Calculate anomalies
#'   cpr_dat$anomaly <- cpr_dat$log_abund - cpr_dat$spline_pred
#'   
#'   #Calculate Anomalies relative to standard deviation in observed log abundance:
#'   #overall_sd <- sd(cpr_dat$spline_pred, na.rm = T)
#'   overall_sd <- sd(cpr_dat$log_abund, na.rm = T)
#'   cpr_dat$rel_anomaly <- cpr_dat$anomaly / overall_sd
#'   
#'   
#'   ####  Calculate Seasonal Averages  ####
#'   #Get Period Split Points from number of season_bins
#'   bin_splits <- c(seq(0, 365, by = ceiling(365 / (season_bins))), 365)
#'   
#'   #Set period number in data based on desired number of splits
#'   period <- data.frame(
#'     period = rep(0, nrow(cpr_dat)),
#'     min_date = rep(NA, nrow(cpr_dat)),
#'     max_date = rep(NA, nrow(cpr_dat)))
#'   
#'   #Add period label and datebound
#'   for (n in 1:nrow(cpr_dat)) {
#'     for (i in 1:season_bins) {
#'       if( cpr_dat$jday[n] > bin_splits[i] & cpr_dat$jday[n] <=  bin_splits[i+1]) {
#'         period[n, "period"]   <- i
#'         period[n, "min_date"] <- bin_splits[i]
#'         period[n, "max_date"] <- bin_splits[i+1]
#'       }
#'     }
#'   }
#'   
#'   # Bind period column and predictions back to original data
#'   period <- period %>% 
#'     mutate(datebounds = str_c(min_date, "-", max_date)) %>% 
#'     select(-c(min_date, max_date))
#'   
#'   cpr_dat <- bind_cols(cpr_dat, period)
#'   
#'   #Get Period Means
#'   seasonal_summary <- cpr_dat %>% 
#'     group_by(year, period, datebounds) %>% 
#'     summarise(
#'       abund_mu = mean(abundance, na.rm = T),
#'       abund_sd = sd(abundance, na.rm = T),
#'       log_abund_mu = log(abund_mu),
#'       log_abund_mu = ifelse(is.infinite(log_abund_mu), 0, log_abund_mu),
#'       anom_mu = mean(anomaly, na.rm = T),
#'       anom_sd = sd(anomaly, na.rm = T),
#'       anom_z = mean(rel_anomaly, na.rm = T),
#'       n_stations = n(),
#'       .groups = "drop") %>% 
#'     ungroup() %>% 
#'     mutate(period = as.character(period))
#'   
#'   #Get Annual Means
#'   annual_summary <- cpr_dat %>% 
#'     group_by(year) %>% 
#'     summarise(
#'       abund_mu = mean(abundance, na.rm = T),
#'       abund_sd = sd(abundance, na.rm = T),
#'       log_abund_mu = log(abund_mu),
#'       log_abund_mu = ifelse(is.infinite(log_abund_mu), 0, log_abund_mu),
#'       anom_mu = mean(anomaly, na.rm = T),
#'       anom_sd = sd(anomaly, na.rm = T),
#'       anom_z = mean(rel_anomaly, na.rm = T),
#'       n_stations = n(),
#'       .groups = "drop") %>% 
#'     mutate(period = "annual",
#'            datebounds = "1-365")
#'   
#'   #Put the annual and period means together
#'   period_summaries <- bind_rows(seasonal_summary, annual_summary) %>% 
#'     arrange(year, period)
#'   
#'   # Return Pre out
#'   ts_out <- list(
#'     "cprdat_predicted" = cpr_dat,
#'     "period_summs"     = period_summaries,
#'     "spline_model"     = cc_spline_mod
#'   )
#'   return(ts_out)
#'   
#'   
#' } 
#' 
#' 
#' 
#' 
#' ####_________________________####
#' 
#' 
#' 
#' # reshape the anomalies, subset into lists by years to include in each set
#' prep_PCA_periods <- function(cpr_anomalies_long, 
#'                              matrix_var = "standardized anomalies",
#'                              use_focal_species = FALSE,
#'                              year_subsets = list("1961-2003" = c(1961,2003))){
#'   
#'   
#'   # Testing:
#'   # tar_load(gom_seasonal_avgs); cpr_anomalies_long <- gom_seasonal_avgs
#'   
#'   
#'   #### a.  Subset to focal species:
#'   if(use_focal_species){
#'     
#'     # focal species used in 2005 paper
#'     species_05 <- c("Calanus I-IV", "Calanus finmarchicus V-VI", "Centropages typicus",
#'                     "Oithona spp.","Para-Pseudocalanus spp.",
#'                     "Metridia lucens",  "Euphausiacea spp.", "phytoplankton color index")
#'     
#'     
#'     # reformat names and filter the focal taxa out
#'     cpr_long <- cpr_anomalies_long %>% 
#'       mutate(taxa = tolower(taxa),
#'              taxa = str_replace_all(taxa, "para_pseu", "para-pseu"),
#'              taxa = str_replace_all(taxa, "_", " "),
#'              taxa = str_replace_all(taxa, "spp", "spp."),
#'              taxa = str_replace_all(taxa, "spp..", "spp.")) %>% 
#'       filter(taxa %in% tolower(species_05))
#'     
#'   }
#'   
#'   
#'   #### b. Re-format for PCA
#'   
#'   # Column to pivot
#'   var_col <- switch (matrix_var,
#'                      "standardized anomalies" = "anom_z",
#'                      "mean anomalies" = "anom_mu",
#'                      "mean abundances" = "abund_mu")
#'   var_col <- sym(var_col)
#'   
#'   
#'   # Pivot wider to return a matrix
#'   cpr_wide <- cpr_long %>% 
#'     select(taxa, year, period, datebounds, anom_z) %>% 
#'     pivot_wider(names_from = taxa, values_from = {{var_col}}) %>% 
#'     janitor::clean_names()
#'   
#'   ####  Filter the years for each group, and grab just the taxa.
#'   subset_years <- map(year_subsets, function(x){
#'     
#'     # filter years
#'     x_years <- filter(cpr_wide, between(year, x[[1]], x[[2]]))
#'     
#'   })
#'   
#'   # return the list of wide data and metadata
#'   return(subset_years)
#'   
#'   
#' }
#' 
#' 
#' 
#' 
#' # Pick whether to use annual/time periods
#' prep_PCA_matrices <- function(period_list, periodicity = "annual"){
#'   
#'   # Testing:
#'   # tar_load(cpr_pca_periods); period_list <- cpr_pca_periods
#'   
#'   
#'   # For any of the period(s) pull abundance matrix and metadata to match
#'   map(period_list, function(period_x){
#'     
#'     # Flag the season/yearly periodicity we want
#'     abundance_data <- switch (periodicity,
#'                               "annual" = filter(period_x, datebounds == "1-365"),
#'                               "seasons"= filter(period_x, datebounds != "1-365"),
#'                               "winter" = filter(period_x, datebounds == "0-92"),
#'                               "spring" = filter(period_x, datebounds == "92-184"),
#'                               "summer" = filter(period_x, datebounds == "184-276"),
#'                               "fall"   = filter(period_x, datebounds == "276-365"))
#'     
#'     # Pull away the metadata
#'     meta_data <- select(abundance_data, year, period, datebounds, `phytoplankton_color_index`)
#'     
#'     
#'     # Pull matrix to pass to PCA
#'     pca_mat <- select(abundance_data, -c(year, period, datebounds, `phytoplankton_color_index`))
#'     
#'     # return metadata to match matrix
#'     return(list(pca_matrix = pca_mat,
#'                 metadata = meta_data))
#'     
#'   })
#'   
#'   
#'   
#' }
#' 
#' 
#' 
#' 
#' #### Perform PCA Return Timeseries and Loadings
#' perform_PCA <- function(pca_matrix, pca_meta){
#'   
#'   # 1. Perform PCA
#'   pca_full <- prcomp(pca_mat, center = F, scale. = F)
#'   
#'   
#'   
#'   # 2. Get the the PC weights for each column in matrix
#'   pc_weights <- rownames_to_column(as.data.frame(pca_full$rotation)) %>% 
#'     dplyr::select(taxa = rowname, PC1, PC2, PC3, PC4)
#'   
#'   # 3. Get percent explained 
#'   pca_sdev <- pca_full$sdev
#'   eigs <- pca_sdev ^ 2
#'   
#'   # put pieces in a table
#'   deviance_df <- rbind(
#'     SD = sqrt(eigs),
#'     Proportion = eigs/sum(eigs),
#'     Cumulative = cumsum(eigs)/sum(eigs))
#'   
#'   # Put the percent explained in a table
#'   pca_dev_out <- data.frame(
#'     "PC1" = str_c(as.character(round(deviance_df[2,1] * 100, 2)), "% of Variance"),
#'     "PC2" = str_c(as.character(round(deviance_df[2,2] * 100, 2)), "% of Variance"),
#'     "PC3" = str_c(as.character(round(deviance_df[2,3] * 100, 2)), "% of Variance"),
#'     "PC4" = str_c(as.character(round(deviance_df[2,4] * 100, 2)), "% of Variance"))
#'   
#'   
#'   # 4. Fill gap years and build timeseries
#'   ### NOTE: PCA direction not multiplied by *1 ####
#'   
#'   #Fill in Year Gap
#'   gap_years <- tibble(year = rep(c(1975, 1976), 2),
#'                       PC = c(rep("First Mode", 2), c(rep("Second Mode", 2))))
#'   
#'   
#'   # Timeseries of first Mode
#'   pc1 <- pca_mat %>%
#'     #Add a filler column because function expects years to be column 1
#'     mutate(filler = NA) %>%
#'     select(filler, everything()) %>%
#'     apply_pca_load(pca_load = .,
#'                    pca_rotations = pca_full$rotation,
#'                    mode_num = 1) %>%
#'     rowSums() %>%
#'     as.data.frame()   %>%
#'     mutate(PC = "First Mode")
#'   
#'   # Get years and fill gaps
#'   colnames(pc1)[1] <- "Principal component value"
#'   pc1 <- bind_cols(year = meta$year, pc1)
#'   pc1 <- full_join(gap_years, pc1, by = c("year", "PC")) %>%
#'     filter(PC == "First Mode") %>%
#'     arrange(year)
#'   
#'   # Do the second Mode
#'   pc2 <- pca_mat %>%
#'     mutate(filler = NA) %>% select(filler, everything()) %>%
#'     apply_pca_load(pca_load = .,
#'                    pca_rotations = pca_full$rotation,
#'                    mode_num = 2) %>%
#'     rowSums() %>%
#'     as.data.frame()  %>%
#'     mutate(PC = "Second Mode")
#'   
#'   colnames(pc2)[1] <- "Principal component value"
#'   pc2 <- bind_cols(year = meta$year, pc2)
#'   pc2 <- full_join(gap_years, pc2, by = c("year", "PC")) %>%
#'     filter(PC == "Second Mode") %>%
#'     arrange(year)
#'   
#'   # Bind PCA Timeseries
#'   pc_modes <- bind_rows(pc1, pc2)
#'   
#'   
#'   #### Outputs
#'   return(
#'     list(
#'       "PC_timeseries" = pc_modes,
#'       "PC_weights" = pc_weights,
#'       "Perc_explained" = pca_dev_out
#'     )
#'   )
#'   
#'   
#'   
#' }
#' 


