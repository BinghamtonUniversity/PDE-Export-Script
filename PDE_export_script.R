library(dplyr)
library(tidyverse)
library(RODBC)
library(DBI)
library(dbplyr)
library(odbc)
library(blastula)
library(writexl)
library(knitr)
library(kableExtra)
library(gt)

dd
tryCatch({
  today <- format(Sys.Date(), "%Y-%m-%d")
  output_path <- paste0("Z:/Shared SAASI/Banner Info/Periodic Data Exports/PDE - R Scripts/Dumps/PDE_R_", today, ".xlsx")
  
  # ---- DB Connection ----
  con <- odbcConnect(dsn = "ODSPROD", uid = Sys.getenv("ods_userid"), pwd = Sys.getenv("ods_pwd"))
  conn <- dbConnect(odbc::odbc(), dsn = "ODSPROD", UID = Sys.getenv("ods_userid"), PWD = Sys.getenv("ods_pwd"))
# stop("Forced test error — checking failure email.")
  
  # ---- STUDENT_BU ----
  query <- "
  SELECT DISTINCT t1.PERSON_UID, 
      t1.ID_NUMBER, 
      t1.FIRST_NAME, 
      t1.LAST_NAME, 
      t1.STUDENT_CLASS_DESC_BOAP,
      t1.ACADEMIC_PERIOD,
      t1.ACADEMIC_PERIOD_DESC,
      t1.ACADEMIC_PERIOD_ADMITTED,
      t1.OFFICIALLY_ENROLLED, 
      t1.CONFIDENTIALITY_IND, 
      t1.DECEASED_STATUS, 
      t1.GENDER_IDENTITY_DESC,
      t1.FED_REPORT_ETHNICITY_CAT_DESC, 
      t1.STUDENT_RESIDENCY_DESC,
      t1.STUDENT_POPULATION_DESC,
      t1.COLLEGE_DESC,
      t1.MAJOR,
      t1.MAJOR_DESC,
      t1.DEPARTMENT_DESC,
      t1.PROGRAM_LEVEL
  FROM ODSMGR.STUDENT_BU t1
  WHERE t1.ACADEMIC_PERIOD = 202590
      AND t1.PRIMARY_PROGRAM_IND = 'Y'
      AND t1.OFFICIALLY_ENROLLED = 'Y'
  ORDER BY t1.ID_NUMBER
  "
  STUDENT_BU.df <- sqlQuery(con, query)
  
  # ---- EMAIL_BU ----
  query <- "
  SELECT 
    t1.ENTITY_UID, 
    t1.EMAIL_ADDRESS
  FROM 
    ODSMGR.EMAIL_BU t1
  LEFT JOIN
    ODSMGR.STUDENT_BU t2
    ON t1.ENTITY_UID = t2.PERSON_UID
  WHERE 
    t1.EMAIL_CODE = 'UNIV'
    AND t1.EMAIL_ADDRESS LIKE '%@binghamton.edu' 
    AND t1.PREFERRED_IND = 'Y'
    AND t2.ACADEMIC_PERIOD = 202590
    AND t2.PRIMARY_PROGRAM_IND = 'Y'
    AND t2.OFFICIALLY_ENROLLED = 'Y'
  "
  EMAIL_BU.df <- sqlQuery(con, query)
  
  WORK.QUERY_FOR_EMAIL_BU_0000 <- sqlQuery(
    con,
    "
    SELECT DISTINCT 
      t1.ENTITY_UID, 
      t1.EMAIL_ADDRESS
    FROM 
      ODSMGR.EMAIL_BU t1
    LEFT JOIN 
      ODSMGR.STUDENT_BU t2
    ON 
      t1.ENTITY_UID = t2.PERSON_UID
    WHERE 
      t1.EMAIL_CODE = 'ERR'
      AND t1.EMAIL_ADDRESS LIKE '%@binghamton.edu%'
      AND t1.EMAIL_COMMENT = 'Created because error not in ID MGT'
      AND REGEXP_LIKE(t1.EMAIL_ADDRESS, '[[:digit:]]')
      AND t2.ACADEMIC_PERIOD = 202590
      AND t2.PRIMARY_PROGRAM_IND = 'Y'
      AND t2.OFFICIALLY_ENROLLED = 'Y'
    ORDER BY 
      t1.ENTITY_UID
    "
  )
  
  STUDENT_EMAILS <- STUDENT_BU.df %>%
    left_join(EMAIL_BU.df, by = c("PERSON_UID" = "ENTITY_UID")) %>%
    mutate(SOURCE = ifelse(!is.na(EMAIL_ADDRESS), "EMAIL_BU", NA)) %>%
    left_join(WORK.QUERY_FOR_EMAIL_BU_0000, 
              by = c("PERSON_UID" = "ENTITY_UID"),
              suffix = c("", ".0000")) %>%
    mutate(
      EMAIL_ADDRESS = ifelse(is.na(EMAIL_ADDRESS), EMAIL_ADDRESS.0000, EMAIL_ADDRESS),
      SOURCE = ifelse(is.na(SOURCE) & !is.na(EMAIL_ADDRESS.0000), "EMAIL_BU_0000", SOURCE)
    ) %>%
    select(-EMAIL_ADDRESS.0000)
  
  # ---- GPA ----
  WORK.CU_GPA <- sqlQuery(
    con,
    "
    SELECT 
      t1.PERSON_UID,
      t1.ID,
      t1.NAME,
      t1.ACADEMIC_STUDY_VALUE,
      t1.QUALITY_POINTS AS Cu_QUALITY_POINTS,
      t1.GPA_CREDITS AS Cu_GPA_CREDITS,
      t1.GPA AS Cu_GPA,
      t2.ACADEMIC_PERIOD,
      t2.ACADEMIC_PERIOD_DESC,
      t2.STUDENT_CLASSIFICATION_BOAP,
      t2.COLLEGE_DESC,
      t2.MAJOR
    FROM 
      ODSMGR.GPA t1
    INNER JOIN 
      ODSMGR.STUDENT_BU t2
    ON 
      t1.PERSON_UID = t2.PERSON_UID
      AND t1.ACADEMIC_STUDY_VALUE = t2.PROGRAM_LEVEL
    WHERE 
      t1.GPA_TYPE = 'I'
      AND t1.GPA_GROUPING = 'C'
      AND t2.ACADEMIC_PERIOD = 202590
      AND t2.PRIMARY_PROGRAM_IND = 'Y'
      AND t2.OFFICIALLY_ENROLLED = 'Y'
    ORDER BY 
      t1.ID
    "
  )
  
  WORK.CU_GPA_EMAILS <- STUDENT_EMAILS %>%
    left_join(WORK.CU_GPA, by = "PERSON_UID")
  
  # ---- EOP ----
  WORK.EOP_STATUS <- sqlQuery(
    con,
    "
    SELECT DISTINCT 
      t1.PERSON_UID, 
      t1.ID_NUMBER, 
      t2.EOP_STATUS_DESCRIPTION, 
      t2.EOP_START_TERM, 
      t2.EOP_END_TERM
    FROM 
      (
        SELECT 
          PERSON_UID, 
          ID_NUMBER, 
          MAX(EOP_STATUS_DATE) AS MAX_of_EOP_STATUS_DATE
        FROM 
          ODSMGR.EOP_BU
        WHERE 
          EOP_STATUS != '2'
        GROUP BY 
          PERSON_UID, ID_NUMBER
      ) t1
    INNER JOIN 
      ODSMGR.EOP_BU t2
    ON 
      t1.PERSON_UID = t2.PERSON_UID 
      AND t1.MAX_of_EOP_STATUS_DATE = t2.EOP_STATUS_DATE
    WHERE 
      t2.EOP_STATUS != '2'
    ORDER BY 
      t1.ID_NUMBER
    "
  )
  
  WORK.CU_GPA_EMAILS <- WORK.CU_GPA_EMAILS %>%
    left_join(WORK.EOP_STATUS %>% select(PERSON_UID, EOP_STATUS_DESCRIPTION), by = "PERSON_UID") %>%
    mutate(EOP_IND = if_else(!is.na(EOP_STATUS_DESCRIPTION), "Y", "N")) %>%
    select(-EOP_STATUS_DESCRIPTION)
  
  # ---- Cohort ----
  COHORT <- sqlQuery(con, "
    SELECT DISTINCT
      *
    FROM ODSMGR.STUDENT_COHORT t1
    LEFT JOIN ODSMGR.STUDENT_BU t2
      ON t1.PERSON_UID = t2.PERSON_UID
      AND t1.ACADEMIC_PERIOD = t2.ACADEMIC_PERIOD
    WHERE t1.ACADEMIC_PERIOD = 202590
      AND t2.PRIMARY_PROGRAM_IND = 'Y'
      AND t2.OFFICIALLY_ENROLLED = 'Y'
      AND t1.COHORT != 'EXCELRECP'
  ")
  
  WORK.CU_GPA_EMAILS_COHORT <- WORK.CU_GPA_EMAILS %>%
    left_join(COHORT %>% select(PERSON_UID, COHORT, COHORT_DESC), by = "PERSON_UID")
  
  # ---- Birthdate ----
  DETAIL <- sqlQuery(con, "
    SELECT DISTINCT
        t1.PERSON_UID,
        TO_CHAR(BIRTH_DATE, 'MM/DD/YYYY') AS DOB,
        t1.FIRST_GENERATION_IND,
        t1.LEGAL_SEX_DESC
    FROM ODSMGR.PERSON_DETAIL_BU t1
    LEFT JOIN ODSMGR.STUDENT_BU t2
    ON t1.PERSON_UID = t2.PERSON_UID
    WHERE t2.ACADEMIC_PERIOD = 202590
    AND t2.PRIMARY_PROGRAM_IND = 'Y'
    AND t2.OFFICIALLY_ENROLLED = 'Y'
  ")
  
  WORK.BIRTH <- WORK.CU_GPA_EMAILS_COHORT %>%
    left_join(DETAIL %>% select(PERSON_UID, DOB, FIRST_GENERATION_IND, LEGAL_SEX_DESC), by = "PERSON_UID")
  
  # ---- Residency ----
  WORK.IS_OOS_INTRNL <- WORK.BIRTH %>%
    mutate(
      IS_OOS_INTRNL = case_when(
        STUDENT_RESIDENCY_DESC %in% c("Resident", "New York High School") ~ "In-State",
        STUDENT_RESIDENCY_DESC == "International" ~ "International",
        TRUE ~ "Out of State"
      )
    )
  
  # ---- Race ----
  RACE <- sqlQuery(con, "
    SELECT DISTINCT
        t1.PERSON_UID,
        t1.SUNY_RACE_ETHNICITY_CODE,
        t1.UNDERREPRESENTED_IND,
        t1.CITIZENSHIP_DESC
    FROM ODSMGR.PERSON_SENSITIVE_IPEDS_BU t1
    LEFT JOIN ODSMGR.STUDENT_BU t2
    ON t1.PERSON_UID = t2.PERSON_UID
    WHERE t2.ACADEMIC_PERIOD = 202590
    AND t2.PRIMARY_PROGRAM_IND = 'Y'
    AND t2.OFFICIALLY_ENROLLED = 'Y'
  ")
  
  WORK.RACE <- WORK.IS_OOS_INTRNL %>%
    left_join(RACE, by = "PERSON_UID") %>%
    mutate(INTRNL = ifelse(SUNY_RACE_ETHNICITY_CODE == "Nonresident", "Y", "N"))
  
  # ---- Address ----
  WORK.ADDRESS <- sqlQuery(con, "
    SELECT DISTINCT 
        t1.ENTITY_UID AS PERSON_UID, 
        t1.STREET_LINE1, 
        t1.STREET_LINE2, 
        t1.STREET_LINE3,
        t1.ADDRESS_TYPE,
        t1.ADDRESS_TYPE_DESC,
        t1.ADDRESS_START_DATE,
        t1.ADDRESS_END_DATE 
    FROM ODSMGR.ADDRESS t1
    LEFT JOIN ODSMGR.STUDENT_BU t2
    ON t1.ENTITY_UID = t2.PERSON_UID
    WHERE t2.ACADEMIC_PERIOD = 202590
    AND t1.ADDRESS_TYPE = 'CA'
    AND t2.PRIMARY_PROGRAM_IND = 'Y'
    AND t2.OFFICIALLY_ENROLLED = 'Y'
    AND (t1.ADDRESS_END_DATE > SYSDATE OR t1.ADDRESS_END_DATE IS NULL)
  ")
  
  ALL <- WORK.RACE %>%
    left_join(WORK.ADDRESS, by = "PERSON_UID") %>%
    mutate(
      HOUSING_TYPE = if_else(ADDRESS_TYPE == "CA", "On-Campus", "Off-Campus", missing = "Off-Campus"),
      COMMUNITY = if_else(ADDRESS_TYPE == "CA", STREET_LINE1, "Off-Campus", missing = "Off-Campus"),
      HALL = if_else(ADDRESS_TYPE == "CA", STREET_LINE2, "Off-Campus", missing = "Off-Campus"),
      ROOM = if_else(ADDRESS_TYPE == "CA", STREET_LINE3, "Off-Campus", missing = "Off-Campus")
    )
  
  ALL_no_dupe_cols <- ALL %>%
    select(-ends_with(".y")) %>%
    rename_with(~ gsub("\\.x$", "", .), ends_with(".x")) %>%
    mutate(BAP_IND = ifelse(MAJOR == "9VA", "Y", "N"))
  
  PDE <- ALL_no_dupe_cols %>%
    select(
      ID_NUMBER, FIRST_NAME, LAST_NAME, EMAIL_ADDRESS, ACADEMIC_PERIOD,
      ACADEMIC_PERIOD_DESC, STUDENT_RESIDENCY_DESC, ACADEMIC_PERIOD_ADMITTED, OFFICIALLY_ENROLLED,
      CONFIDENTIALITY_IND, DECEASED_STATUS, STUDENT_POPULATION_DESC,
      STUDENT_CLASS_DESC_BOAP, COHORT, COLLEGE_DESC, MAJOR, MAJOR_DESC,
      DEPARTMENT_DESC, BAP_IND, EOP_IND, FIRST_GENERATION_IND, INTRNL,
      IS_OOS_INTRNL, LEGAL_SEX_DESC, GENDER_IDENTITY_DESC, DOB,
      SUNY_RACE_ETHNICITY_CODE, UNDERREPRESENTED_IND,
      ACADEMIC_STUDY_VALUE, CU_GPA, HOUSING_TYPE, COMMUNITY, HALL, ROOM
    )

  write_xlsx(PDE, output_path)
  
  # ---- Success Email ----
  # ---- Summary Table as HTML ----
  summary_df <- PDE %>%
    count(STUDENT_POPULATION_DESC, name = "Count") %>%
    mutate(
      Percent = round(Count / sum(Count) * 100, 1)
    ) %>%
    arrange(desc(Count))
  
  summary_df <- bind_rows(
    summary_df,
    tibble(
      STUDENT_POPULATION_DESC = "**Total**",
      Count = sum(summary_df$Count),
      Percent = 100
    )
  )
  
  summary_html <- summary_df %>%
    rename(
      `Population` = STUDENT_POPULATION_DESC
    ) %>%
    kable("html", escape = FALSE, align = "lrr", caption = "Student Population Breakdown") %>%
    kable_styling("striped", full_width = FALSE)
  
  
  
  # ---- Success Email ----
  email <- compose_email(
    body = html(paste0(
      "<p>✅ PDE export completed successfully on ", Sys.Date(), ".</p>",
      "<p><strong>Output file:</strong><br>", output_path, "</p>",
      summary_html,
      "<p><strong>View Full Shiny App:</strong><br>", " Z:/Shared SAASI/Matthew/PDE Shiny App/R Scripts </a></p>"
    )),
    footer = "— Automated PDE Script"
  )
  
  
  
  smtp_send(
    email,
    from = "mjacob28@binghamton.edu",
    to = c("mjacob28@binghamton.edu", 'ewalsh@binghamton.edu'),
    subject = paste("✅ PDE Export Success:", Sys.Date()),
    credentials = creds_file("Z:/Shared SAASI/Banner Info/Periodic Data Exports/PDE - R Scripts/gmail_creds")
  )
  
  close(con)
  dbDisconnect(conn)
  
}, error = function(e) {
  
  # ---- Failure Email ----
  error_email <- compose_email(
    body = md(paste0(
      "❌ *PDE export failed on ", Sys.Date(), "*.\n\n",
      "**Expected output file:**  \n",
      output_path, "\n\n",
      "**Error message:**\n\n",
      "```\n", e$message, "\n```"
    )),
    footer = "— Automated PDE Script"
  )
  
  
  smtp_send(
    error_email,
    from = "mjacob28@binghamton.edu",
    to = c("mjacob28@binghamton.edu","ewalsh@binghamton.edu"),
    subject = paste("❌ PDE Export Failed:", Sys.Date()),
    credentials = creds_file("Z:/Shared SAASI/Banner Info/Periodic Data Exports/PDE - R Scripts/gmail_creds")
  )
})
