# 1- Packages
# Load the library ----
library(tidyverse)        # Core Libraries - dplyr, ggplot2
library(lubridate)        # Library for manipulating date
library(skimr)            # Library for summary statistic of the data
library(plotly)           # Library for interactive plots
library(hrbrthemes)       # A theme library
library(janitor)          # Library for cleaning data

###########################
# 2- Data import and Cleaning  ----
############################
download_speed_measurement <- read.csv('../data/download_speed_measurements.csv') %>%  as_tibble()
upload_speed_measurement <- read.csv('../data/upload_speed_measurements.csv') %>% as_tibble()
details_for_each_person <- read.csv('../data/details_for_each_person.csv') %>%  as_tibble()

##################################################
# Cleaning: Check for duplicate rows in the data with janitor package
#           General summary of the input data with the skimr package
#################################################

# Check we do not have duplicate rows
download_speed_measurement %>% get_dupes()
upload_speed_measurement %>% get_dupes()
details_for_each_person %>%  get_dupes()

#Observation: No duplicate rows found in all imported raw files


# We report on the summary statistics of the raw imported files
download_speed_measurement %>% skimr::skim()
# Observation: There download speed table contains 191200 rows and 4 column
#              The data is made up of three variable type (Character, logical and numeric)
#              The Character and Logical columns do not have missing and looks good
#              The numeric measured_download_speed_in_Mbps columns contain missing data (0.20%)
#
upload_speed_measurement %>%  skimr::skim()
# Observation: There upload speed table contains 159447 rows and 4 column
#              The data is made up of three variable type (Character, logical and numeric)
#              The Character and Logical columns do not have missing and looks good
#              The numeric measured_upload_speed_in_Mbps columns contain missing data (0.20%)
details_for_each_person %>%  skimr::skim()
# Observation: There details for each person table contains 291 rows and 4 column
#              The data is made up of two variable type (Character and numeric)
#              The Character and numeric columns do not have missing and looks good

###########################################
# Before merging the raw input files.
###########################################
# Observation: There is no overlap between the download and upload time (time_of_measurement)
#              There is overlap between the download and upload person identifier (person_id)
# Conclusion: We expect the download and upload speed to be optimal since they were performed
#             at different times.

# Observation: From the DATA_DICTIONARY.md we observe that download_speed_measurement and details_for_each_person
#              have a column in common, the person_id column
#              From the DATA_DICTIONARY.md we observe that upload_speed_measurement and details_for_each_person
#              have a column in common, the person_id column
# Action: For simplicity we will merge the download-user details
#         and upload-user-detail data separately.
#         We will proceed as follows:
#         1- Merge download and users details tables using the person_id as a common field
#         2- Merge Upload and users details tables using the person_id as common field
#
#####################################################


download_speed_user_detail <- download_speed_measurement %>%
    # Join by the common column (person_id)
    left_join(details_for_each_person, by = c("person_id" = "person_id"))

upload_speed_user_detail <- upload_speed_measurement %>%
    # Join by person_id the common
    left_join(details_for_each_person, by = c("person_id" = "person_id"))

##########################################################
# We are require to filter the data on:
#       1- user living in specific cities,
#       2- where measurement was succesfull,
#       3- where test were conducted after specific dates
#Since we decided to process the download-user-details and upload-users_detail separately
# to avoid repeating the similar processing steps we will define some functions to
# facilitate the works
# Action: Define two function:
#         1-filter_by_city_date_test_status (filter the data on specified conditions)
#         2-calculate_speed_percentile_perunit_time (Calculate the nth percentile per defined unit time (hour, day, week, month))
#         3-calculate_speed_average_per_variables (Calculate the average download or upload speed per user)
####################################################

######################################
# Function:
######################################
# filter_by_city_date_test_status function: (filter the data on specified conditions)
filter_by_city_date_test_status <-
    function(data= data,                             # input data frame
             date = '2020-12-31',                    # date: Default Date above which to select data
             cities = c("Samsville", "Databury"),    # cities: Default cities to filter data by
             success = TRUE){                        # Success: Whether the test was a success (TRUE or FALSE)
        data <- data %>% filter(city %in% cities) %>%
            # Filter on successful test
            filter(did_test_complete_successfully == success) %>%
            # Convert the character type time_of_measurement into datetime data type using ymd_hms from lubridate
            mutate(time_of_measurement = ymd_hms(time_of_measurement)) %>%
            # select rows where measurement was done after a specific date
            filter(time_of_measurement > ymd(date))
        return (data)
    }

# calculate_speed_percentile_perunit_time function: Calculate the nth percentile per defined unit time (hour, day, week, month))
calculate_speed_percentile_perunit_time  <-
    function(data = data,                                   # Input data frame
             measurement = measured_download_speed_in_Mbps, # unquote column name of speed measurement(download/upload)
             unit_time = "day",                             # Unit time to compute percentile from
             probs = .60){                                  # quantile probability with value in [0,1]
        # prepare the measurement to be used inside tidy methods
        measurement_expr <- enquo(measurement)
        data <- data %>%  mutate(date = time_of_measurement %>%
                                     floor_date(unit = unit_time)) %>%
            group_by(person_id, date) %>%
            # Calculate the mean upload or download speed per person per date(day)
            summarize(average_measurement = mean(!!measurement_expr)) %>%
            ungroup() %>%
            group_by(person_id) %>%
            # Calculate the nth percentile download or upload speed per user
            summarize(mesurement_percentile = quantile(average_measurement, probs= probs)) %>%
            ungroup()
        return(data)

    }
# calculate_speed_average_per_variables function: Calculate the average download or upload speed per user)
calculate_speed_average_per_variables <-
    function (data = data,                                      # input data frame
              ...,                                              # unquote variables to group by
              measurement = measured_download_speed_in_Mbps){   # unquote column name of speed measurement
        measurement_expr <- enquo(measurement)
        groupby_vars_expr <- quos(...)
        data <- data %>%
            group_by(!!! groupby_vars_expr) %>%
            summarise(average_measurement = mean(!!measurement_expr)) %>%
            ungroup()
        return(data)
    }


####################################################################################
# Import these files and combine them into a single table which as the structure
# person_id, city, type_of_broadband_connection, name_of_isp, average_download_speed
######################################################################################

successfull_download_tbl <-
    download_speed_user_detail %>%
    # Filter dataframe on given date, cities and measurement status
    filter_by_city_date_test_status(date = '2020-12-31',
                                    cities = c("Samsville", "Databury"),
                                    success = TRUE)

successfull_upload_tbl <- upload_speed_user_detail %>%
    filter_by_city_date_test_status(date = '2020-12-31',
                                    cities = c("Samsville", "Databury"),
                                    success = TRUE)


successfull_download_tbl
successfull_upload_tbl

download_upload_speed_users_details_tbl <- successfull_download_tbl %>%
    # Calculate the average download speed per users
    calculate_speed_average_per_variables(person_id,
                                          city,
                                          type_of_broadband_connection,
                                          name_of_isp) %>%
    # Rename the average download speed column name
    rename(average_download_speed_in_Mbps = average_measurement) %>%
    # Calculate and append the average upload speed per user
    left_join(successfull_upload_tbl %>%
                  calculate_speed_average_per_variables(
                      person_id ,
                      measurement = measured_upload_speed_in_Mbps) %>%
                  rename(average_upload_speed_in_Mbps=average_measurement)) %>%
    # Optional: Calculate and append the 60th percentile download speed per day
    left_join(successfull_download_tbl %>%
                  calculate_speed_percentile_perunit_time(
                      unit_time="day",
                      measurement = measured_download_speed_in_Mbps)) %>%
    # Rename the 60th percentile download speed column name
    rename(percentile60_download_speed_in_Mbps = mesurement_percentile)

# Use skimr to report on various summary statistics of the dataset
download_upload_speed_users_details_tbl %>% skimr::skim()

# Observation: There are no missing values in the filtered and combine download upload and user details for the
# as shown by the n_missing column of the skimr output


#####################################
# Data quality  ----
######################################

# We report on the summary statistics of the raw imported files and combined files
download_speed_user_detail %>% skimr::skim()
# Observation: there are (1-0.803)= 0.20% of missing measured_donwload_speed_in_Mbps
upload_speed_user_detail %>%  skimr::skim()
# Observation: there are (1-0.803)= 0.20% of missing measured_upload_speed_in_Mbps
download_upload_speed_users_details_tbl %>%  skimr::skim()
# Observation: there are no missing measured speed on the selected subset of dataset containing
# users living in Databury and Samsville for measurement performed from January 2021


######################################################
# Plot the relationship of download to upload
#######################################################
p <- download_upload_speed_users_details_tbl %>%
    # Data manipulation: Construct a label for interactive plot
    mutate(label_text = str_glue('person_id={person_id}
                                 download={round(percentile60_download_speed_in_Mbps,2)}
                                 upload={round(average_upload_speed_in_Mbps,2)}')) %>%
    # Data Visualization
    ggplot(aes(percentile60_download_speed_in_Mbps, average_upload_speed_in_Mbps,
               text = label_text ,
               color=type_of_broadband_connection)) +
    geom_jitter(width = 0.9, height = 0.9) +
    facet_wrap(city ~ name_of_isp) + theme_bw()

# Interactive plot
ggplotly(p, tooltip = "text")


#Observation: There are mislabel download speed for ADSL vith values greater than 25mb
#             There are mislabled download speed for Fibre with value less than 25mb
#             Move the point on the plot to get the details of mislabeled data
# Action plan: Get rid of these values

person_with_mislabeled_connection_type <- bind_rows(
    download_upload_speed_users_details_tbl %>% filter(type_of_broadband_connection == "ADSL" ,
                                                       percentile60_download_speed_in_Mbps > 25),
    download_upload_speed_users_details_tbl %>% filter(type_of_broadband_connection == "Fibre" ,
                                                       percentile60_download_speed_in_Mbps <  25)) %>%
    pull(person_id)

person_with_mislabeled_connection_type

################################
# Filter out mislabeled data
################################
download_upload_speed_users_details_tbl <- download_upload_speed_users_details_tbl %>%
    filter(!person_id %in% person_with_mislabeled_connection_type )



########################################################
#   4- Data Summarization
########################################################
# 1 Difference in download between ISP for each connection type
# Since we will calculate the difference in download and upload between ISP for each
# Connection type, let us define a function to facilitate this process

# A function to summarize in table the download or upload speed difference between ISP
speed_difference_btw_isp <- function(data = data,
                                     ...,
                                     measurement = percentile60_download_speed_in_Mbps ){
    groupby_vars_expr <- quos(...)
    measurement_expr <- enquo(measurement)
    if (quo_name(measurement_expr) %in% colnames(data) ){
        data <- data %>%
            select(type_of_broadband_connection, name_of_isp, city, !! measurement_expr)  %>%
            group_by(!!! groupby_vars_expr) %>%
            summarize(mean_measurement = mean(!! measurement_expr)) %>%
            ungroup() %>%
            pivot_wider(names_from = name_of_isp,
                        values_from = mean_measurement)
    }
    else{
        stop(str_glue("measurement = {measurement_expr} is not a permitted option." ))
    }

    return(data)
}

# Difference in Download between ISP by connection type
difference_in_download_btw_isp <- download_upload_speed_users_details_tbl %>%
    speed_difference_btw_isp(type_of_broadband_connection,
                             name_of_isp,
                             measurement = percentile60_download_speed_in_Mbps )

# Difference in Download between ISP by connection type by city
difference_in_download_btw_isp_by_city <- download_upload_speed_users_details_tbl %>%
    speed_difference_btw_isp(type_of_broadband_connection,
                             name_of_isp,
                             city,
                             measurement = percentile60_download_speed_in_Mbps )

# Difference in upload speed between ISP by connection type
difference_in_upload_btw_isp <- download_upload_speed_users_details_tbl %>%
    speed_difference_btw_isp(type_of_broadband_connection,
                             name_of_isp,
                             measurement = average_upload_speed_in_Mbps )

# Difference in Upload speed between ISP by connection type by city
difference_in_upload_btw_isp_by_city <- download_upload_speed_users_details_tbl %>%
    speed_difference_btw_isp(type_of_broadband_connection,
                             name_of_isp,
                             city,
                             measurement = average_upload_speed_in_Mbps )

#######################################################################################
# 4 a) Difference in download and download speed between ISP for each connection type
# a) Make a couple of brief comment to
#    summarize the finding
#######################################################################################
difference_in_download_btw_isp
# Observation: Overall Fibrelicious ISP offers a better download speed compare to Useus ISP
# We will need to hypothesis test (t-test) the difference in speed we observe to assertain that this
# difference is statistically significant
difference_in_download_btw_isp_by_city
# Observation: Overall Fibrelicious ISP offers a better download speed compare to Useus ISP
# but for users living in Samsville and opting for ADSL Useus offers a better download speed
# We will need to hypothesis test (t-test) the difference in speed we observe to assertain that this
# difference is statistically significant

difference_in_upload_btw_isp
# Observation: Overall Fibrelicious ISP offers a better upload speed compare to Useus ISP
# We will need to hypothesis test (t-test) the difference in speed we observe to assertain that this
# difference is statistically significant
difference_in_upload_btw_isp_by_city
# Observation: Overall Fibrelicious ISP offers a better download speed compare to Useus ISP
# but for users living in Samsville and opting for ADSL Useus offers a better Upload speed
# We will need to hypothesis test (t-test) the difference in speed we observe to assertain that this
# difference is statistically significant



#########################################
# b) Visualization of distribution of average
# download speed by ISP by city by connection type
#########################################

download_upload_speed_users_details_tbl %>%
    select(type_of_broadband_connection,
           name_of_isp,
           city,
           percentile60_download_speed_in_Mbps) %>%
    ggplot(aes(x= percentile60_download_speed_in_Mbps, fill=type_of_broadband_connection)) +
    geom_histogram(binwidth =2, alpha=.5, position ="identity") + facet_wrap(name_of_isp~ city) +
    labs(title ="Distribution of Average Download Speed by \nISP by City by Connection type",
         x="Average download speed Mpbs",
         y = "Number of Users",
         fill = "Connection type",
         caption = "SamKnows Data analyst Test") +
    theme_ipsum()

###############################################################
# Observation: Regardless of the city, the broadband download speed are within the expected range
#              There is not much difference between ADSL and VDSL donwload speed in Databury from Useus ISP
#              Fibre download speed are higher from Fibrelicious compare to Useus regardless of the city
#              For User is living in Databury and willing to get a Fiber connection, Fibrelicious offers a faster connection compare to Useus
# Action: For those with a limited budget in Databury, assuming fast download speed correspond to higher cost
#         There is no value for money buying the expensive packet VDSL compare to the cheaper ADSL,
#         the download speed are almost the same.
#         - For users living in both Databury and Samsville, Fibrelicious ISP offers a faster download
#           connection compare to Useus ISP
###############################################################

###############################################################
# c) From the distribution of the download shown above
###############################################################
difference_in_download_btw_isp_by_city

#Observation: A Customer living in Databury and having a Fibre connection
# will have a better connection speed from the ISP provider Fibrelicious.
# The average fibre connection speed from Fibrelicious in Databury is
# 209 Mbps



##################################################
# d) Optional --- Difference in download speed by ISP
##################################################

download_by_isp_plot <- successfull_download_tbl %>%
    filter(!person_id %in% person_with_mislabeled_connection_type ) %>%
    select(time_of_measurement,
           measured_download_speed_in_Mbps,
           city,
           type_of_broadband_connection,
           name_of_isp) %>%
    mutate(time_of_the_day = hour(time_of_measurement)) %>%
    group_by(type_of_broadband_connection, name_of_isp, time_of_the_day) %>%
    summarize(measured_download_speed_in_Mbps = mean(measured_download_speed_in_Mbps)) %>%
    ungroup() %>%
    mutate(label_text = str_glue("Time: {time_of_the_day} o'clock
                                 download speed = {round(measured_download_speed_in_Mbps,2)} Mbps")) %>%
    ggplot(aes(time_of_the_day,
               measured_download_speed_in_Mbps,
               text = label_text,
               color=type_of_broadband_connection,
               group=type_of_broadband_connection)) +
    geom_point() + geom_line() +
    labs(title="Download by ISP at different time of the day",
         x = "Time of the day (hours)",
         y = "Average download speed in Mbps",
         fill = "Connection Type",
         caption = "SamKnows Data analyst assessment test") +
    facet_wrap(name_of_isp~type_of_broadband_connection, scales = "free", nrow = 2, ncol = 3) +
    theme_ipsum() +
    theme(legend.position = "none")

download_by_isp_plot
ggplotly(download_by_isp_plot, tooltip = "text")



###############################################################################################
# Observation: 1-Downloads are low between 6pm and 10pm and high throughout 12am and 5pm
#              2-I expected a peak download mainly at working hours 9am-5pm but earlier (12am-9am)
#             peak might be due to user spanning different timezones
################################################################################################




#
#----------------------------------Done ------------------------------------------------
#
