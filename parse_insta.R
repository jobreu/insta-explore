# load required libraries ####

library(dplyr)     
library(readr)     
library(lubridate) 
library(jsonlite)  
library(purrr)     
library(stringr)

# helper function to safely pluck from list-like elements ####

safe_pluck_from_list <- function(.x, ..., .default = NA_character_) {
  if (is.list(.x)) {
    # If .x is a list, try to pluck the element
    val <- purrr::pluck(.x, ..., .default = .default)
    # Ensure NULL is converted to NA
    if (is.null(val)) return(.default)
    return(as.character(val))
  } else {
    # If .x is not a list (e.g., a string, NA, or NULL), return default
    return(.default)
  }
}

# helper function for processing single lines from JSON ####

process_post_line <- function(json_line, line_index) {
  tryCatch({
    post_data <- jsonlite::fromJSON(json_line, flatten = FALSE)
    
    
    # --- Extract Media URLs ---
    media_type_val <- purrr::pluck(post_data, "data", "media_type", .default = NA_integer_)
    num_media_val <- 1L # Default for Image/Video
    
    if (!is.null(media_type_val) && !is.na(media_type_val)) {
      if (media_type_val == 1) { # Image
        num_media_val <- 1L
        
      } else if (media_type_val == 2) { # Video
        num_media_val <- 1L
        
      } else if (media_type_val == 8) { # Carousel
        num_media_val <- purrr::pluck(post_data, "data", "carousel_media_count", .default = NA_integer_)
      }
    }
    
    # --- Extract Hashtags ---
    caption_text <- as.character(purrr::pluck(post_data, "data", "caption", "text", .default = NA_character_))
    hashtags_val <- NA_character_
    if (!is.na(caption_text)) { # Only run regex if caption is not NA
      tags <- stringr::str_extract_all(caption_text, "#[\\w\\.]+")[[1]]
      if (length(tags) > 0) {
        hashtags_val <- paste(tags, collapse = ", ")
      }
    }
    
    # --- Extract Usertags ---
    usertags_list <- purrr::pluck(post_data, "data", "usertags")
    usertags_val <- NA_character_
    
    if (!is.null(usertags_list) && is.list(usertags_list) && length(usertags_list) > 0) {
      # Use safe_pluck_from_list, which checks if .x is a list
      usertags_vec <- unlist(purrr::map(usertags_list, ~ safe_pluck_from_list(.x, "user", "username", .default = NA_character_)))
      usertags_vec_clean <- usertags_vec[!is.na(usertags_vec)]
      usertags_val <- ifelse(length(usertags_vec_clean) > 0, paste(usertags_vec_clean, collapse = ", "), NA_character_)
    }
    
    # --- Safely pre-pluck values for building tibble ---
    unix_ts_val <- as.numeric(purrr::pluck(post_data, "data", "taken_at", .default = NA_real_))
    lat_val <- purrr::pluck(post_data, "data", "location", "lat", .default = NA_real_)
    lng_val <- purrr::pluck(post_data, "data", "location", "lng", .default = NA_real_)
    
    # --- ID ---
    id_val <- as.character(purrr::pluck(post_data, "data", "code", .default = NA_character_))
    
    
    # --- Build Tibble ---
    df <- tibble::tibble(
      id = id_val, 
      post_source_domain = as.character(purrr::pluck(post_data, "source_platform", .default = NA_character_)),
      timestamp = lubridate::as_datetime(unix_ts_val, origin = "1970-01-01"), # create proper timestamp
      url = if_else(!is.na(id_val), 
                    paste0("https://www.instagram.com/p/", id_val, "/"), 
                    NA_character_),
      body = caption_text, # Re-use from hashtag extraction
      author = as.character(purrr::pluck(post_data, "data", "user", "username", .default = NA_character_)),
      author_fullname = as.character(purrr::pluck(post_data, "data", "user", "full_name", .default = NA_character_)),
      verified = purrr::pluck(post_data, "data", "user", "is_verified", .default = NA),
      media_type = dplyr::case_when(
        media_type_val == 1 ~ "Image",
        media_type_val == 2 ~ "Video",
        media_type_val == 8 ~ "Carousel",
        TRUE ~ as.character(media_type_val)
      ),
      num_media = as.integer(num_media_val),
      hashtags = hashtags_val,
      usertags = usertags_val,
      likes_hidden = purrr::pluck(post_data, "data", "like_and_view_counts_disabled", .default = NA),
      num_likes = as.integer(purrr::pluck(post_data, "data", "like_count", .default = NA_integer_)),
      num_comments = as.integer(purrr::pluck(post_data, "data", "comment_count", .default = NA_integer_)),
      location_name = as.character(purrr::pluck(post_data, "data", "location", "name", .default = NA_character_)),
      location_latlong = if_else(
        !is.na(lat_val) & !is.na(lng_val),
        paste(lat_val, lng_val, sep = ", "),
        NA_character_
      ),
      unix_timestamp = unix_ts_val
    )
    
    return(df)
    
  }, error = function(e) {
    warning(paste("Failed to process line", line_index, ". Error:", e$message))
    return(tibble())
  })
}


# function for parsing Instagram NDJSON file into a clean dataframe
# reads a newline-delimited JSON (NDJSON) file containing Instagram post metadata,
# flattens each line, and extracts a specific set of 18 variables

parse_instagram_posts <- function(filepath) {
  
  # --- Step 1: Read all lines ---
  message("Reading NDJSON file line by line...")
  tryCatch({
    all_lines <- readLines(filepath, warn = FALSE) # Suppress warnings about incomplete final line
  }, error = function(e) {
    stop(paste("Failed to read file:", e$message))
  })
  
  # Filter out any potential empty lines
  all_lines <- all_lines[all_lines != ""]
  
  if (length(all_lines) == 0) {
    warning("File is empty or contains only empty lines.")
    return(tibble())
  }
  
  message(paste("Parsing", length(all_lines), "posts..."))
  
  # --- Step 2: Process each line and row-bind ---
  # Use purrr::imap_dfr to pass both the line content (.x) and the index (.y)
  # to the helper function
  final_tibble <- purrr::imap_dfr(all_lines, process_post_line)
  
  if(is.null(final_tibble) || nrow(final_tibble) == 0) {
    warning("No data was successfully parsed. All lines may have failed. Check warnings.")
    return(tibble())
  }
  
  message(paste("Successfully processed", nrow(final_tibble), "posts."))
  return(final_tibble)
}