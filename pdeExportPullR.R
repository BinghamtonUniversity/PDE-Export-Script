# =====================================
# PDE Export (PullR Version)
# =====================================

# ---- Libraries ----
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
library(glue)
library(pullr)
library(pins)

resetPullResults()

get_data_path <- function(relative_path) {
  if (.Platform$OS.type == "windows") {
    base_path <- "//bushare.binghamton.edu/assess"
  } else {
    base_path <- "/mnt/assess/"
  }
  file.path(base_path, relative_path)
}

# ---- Setup ----
today <- format(Sys.Date(), "%Y-%m-%d")

envvar_path <- get_data_path("Shiny Apps/.Renviron.R")
readRenviron(envvar_path)

pin_path <- get_data_path("Data Hub/PDE_APP_SP2026/Data")

dump_path_xlsx <- get_data_path(paste0("Shared SAASI/Banner Info/Periodic Data Exports/PDE - R Scripts/Dumps/PDE_SP_2026_R_", today, ".xlsx"))
dump_path_rds <- get_data_path(paste0("Shared SAASI/Banner Info/Periodic Data Exports/PDE - R Scripts/Dumps_RDS/PDE_SP_2026_R_", today, ".rds")) 

board <- board_folder(
  path      = pin_path,
  versioned = TRUE
)

academic_period <- 202620
script_loc <- "PDE_APP_SP2026/Migration/pdeExportPullR.R"
end_loc <- "PDE_APP_SP2026"

# ---- DB Connection ----
con  <- odbcConnect(dsn = "ODSPROD", uid = Sys.getenv("ods_userid"), pwd = Sys.getenv("ods_pwd"))
conn <- dbConnect(odbc::odbc(), dsn = "ODSPROD", UID = Sys.getenv("ods_userid"), PWD = Sys.getenv("ods_pwd"))

# =====================================
# 1. STUDENT_BU
# =====================================
STUDENT_BU_QUERY <- glue("
SELECT DISTINCT t1.PERSON_UID, t1.ID_NUMBER, t1.FIRST_NAME, t1.LAST_NAME,
    t1.STUDENT_CLASS_DESC_BOAP, t1.ACADEMIC_PERIOD, t1.ACADEMIC_PERIOD_DESC,
    t1.ACADEMIC_PERIOD_ADMITTED, t1.OFFICIALLY_ENROLLED, t1.CONFIDENTIALITY_IND,
    t1.DECEASED_STATUS, t1.GENDER_IDENTITY_DESC, t1.FED_REPORT_ETHNICITY_CAT_DESC,
    t1.STUDENT_RESIDENCY_DESC, t1.STUDENT_POPULATION_DESC, t1.COLLEGE_DESC,
    t1.MAJOR, t1.MAJOR_DESC, t1.DEPARTMENT_DESC, t1.PROGRAM_LEVEL
FROM ODSMGR.STUDENT_BU t1
WHERE t1.ACADEMIC_PERIOD = {academic_period}
  AND t1.PRIMARY_PROGRAM_IND = 'Y'
  AND t1.OFFICIALLY_ENROLLED = 'Y'
ORDER BY t1.ID_NUMBER
")

STUDENT_BU.df <- run_with_retries(
  fun = function() {
    sqlQuery(con, STUDENT_BU_QUERY, errors = FALSE)
  },
  pull_name = "STUDENT_BU",
  script_loc = script_loc,
  end_loc = end_loc,
  columns = c(
    "PERSON_UID","ID_NUMBER","FIRST_NAME","LAST_NAME","STUDENT_CLASS_DESC_BOAP",
    "ACADEMIC_PERIOD","ACADEMIC_PERIOD_DESC","ACADEMIC_PERIOD_ADMITTED",
    "OFFICIALLY_ENROLLED","CONFIDENTIALITY_IND","DECEASED_STATUS",
    "GENDER_IDENTITY_DESC","FED_REPORT_ETHNICITY_CAT_DESC","STUDENT_RESIDENCY_DESC",
    "STUDENT_POPULATION_DESC","COLLEGE_DESC","MAJOR","MAJOR_DESC",
    "DEPARTMENT_DESC","PROGRAM_LEVEL"
  ),
  affects = "pde",
  max_tries = 3
)

# =====================================
# 2. EMAIL_BU + EMAIL_BU_0000
# =====================================
EMAIL_BU.df <- run_with_retries(
  fun = function() {
    sqlQuery(con, glue("
      SELECT t1.ENTITY_UID, t1.EMAIL_ADDRESS
      FROM ODSMGR.EMAIL_BU t1
      LEFT JOIN ODSMGR.STUDENT_BU t2 ON t1.ENTITY_UID = t2.PERSON_UID
      WHERE t1.EMAIL_CODE = 'UNIV'
        AND t1.EMAIL_ADDRESS LIKE '%@binghamton.edu'
        AND t1.PREFERRED_IND = 'Y'
        AND t2.ACADEMIC_PERIOD = {academic_period}
        AND t2.PRIMARY_PROGRAM_IND = 'Y'
        AND t2.OFFICIALLY_ENROLLED = 'Y'
    "), errors = FALSE)
  },
  pull_name = "EMAIL_BU",
  script_loc = script_loc,
  end_loc = end_loc,
  columns = c("ENTITY_UID","EMAIL_ADDRESS"),
  affects = "pde",
  max_tries = 3
)

EMAIL_BU_0000.df <- run_with_retries(
  fun = function() {
    sqlQuery(con, glue("
      SELECT DISTINCT t1.ENTITY_UID, t1.EMAIL_ADDRESS
      FROM ODSMGR.EMAIL_BU t1
      LEFT JOIN ODSMGR.STUDENT_BU t2 ON t1.ENTITY_UID = t2.PERSON_UID
      WHERE t1.EMAIL_CODE = 'ERR'
        AND t1.EMAIL_ADDRESS LIKE '%@binghamton.edu%'
        AND t1.EMAIL_COMMENT = 'Created because error not in ID MGT'
        AND REGEXP_LIKE(t1.EMAIL_ADDRESS, '[[:digit:]]')
        AND t2.ACADEMIC_PERIOD = {academic_period}
        AND t2.PRIMARY_PROGRAM_IND = 'Y'
        AND t2.OFFICIALLY_ENROLLED = 'Y'
    "), errors = FALSE)
  },
  pull_name = "EMAIL_BU_0000",
  script_loc = script_loc,
  end_loc = end_loc,
  columns = c("ENTITY_UID","EMAIL_ADDRESS"),
  affects = "pde",
  max_tries = 3
)


if (exists("STUDENT_BU.df") &&
    is.data.frame(STUDENT_BU.df) &&
    is.data.frame(EMAIL_BU.df) && nrow(EMAIL_BU.df) > 0 &&
    is.data.frame(EMAIL_BU_0000.df) && nrow(EMAIL_BU_0000.df) > 0) {
  
  STUDENT_EMAILS <- STUDENT_BU.df %>%
    left_join(EMAIL_BU.df, by = c("PERSON_UID" = "ENTITY_UID")) %>%
    mutate(SOURCE = ifelse(!is.na(EMAIL_ADDRESS), "EMAIL_BU", NA)) %>%
    left_join(EMAIL_BU_0000.df, by = c("PERSON_UID" = "ENTITY_UID"), suffix = c("", ".0000")) %>%
    mutate(
      EMAIL_ADDRESS = ifelse(is.na(EMAIL_ADDRESS), EMAIL_ADDRESS.0000, EMAIL_ADDRESS),
      SOURCE = ifelse(is.na(SOURCE) & !is.na(EMAIL_ADDRESS.0000), "EMAIL_BU_0000", SOURCE)
    ) %>%
    select(-EMAIL_ADDRESS.0000)
  
}

# =====================================
# 3. GPA
# =====================================
GPA.df <- run_with_retries(
  fun = function() {
    sqlQuery(con, glue("
      SELECT t1.PERSON_UID, t1.ID, t1.NAME, t1.ACADEMIC_STUDY_VALUE,
             t1.QUALITY_POINTS AS CU_QUALITY_POINTS, t1.GPA_CREDITS AS CU_GPA_CREDITS,
             t1.GPA AS CU_GPA, t2.STUDENT_CLASSIFICATION_BOAP
      FROM ODSMGR.GPA t1
      INNER JOIN ODSMGR.STUDENT_BU t2
        ON t1.PERSON_UID = t2.PERSON_UID
       AND t1.ACADEMIC_STUDY_VALUE = t2.PROGRAM_LEVEL
      WHERE t1.GPA_TYPE = 'I'
        AND t1.GPA_GROUPING = 'C'
        AND t2.ACADEMIC_PERIOD = {academic_period}
        AND t2.PRIMARY_PROGRAM_IND = 'Y'
        AND t2.OFFICIALLY_ENROLLED = 'Y'
    "), errors = FALSE)
  },
  pull_name = "GPA",
  script_loc = script_loc,
  end_loc = end_loc,
  columns = c(
    "PERSON_UID","ID","NAME","ACADEMIC_STUDY_VALUE","CU_QUALITY_POINTS",
    "CU_GPA_CREDITS","CU_GPA","STUDENT_CLASSIFICATION_BOAP"
  ),
  affects = "pde",
  max_tries = 3
)


if (exists("STUDENT_EMAILS") &&
    is.data.frame(STUDENT_EMAILS) &&
    is.data.frame(GPA.df) && nrow(GPA.df) > 0) {
  
  WORK.CU_GPA_EMAILS <- STUDENT_EMAILS %>%
    left_join(GPA.df, by = "PERSON_UID")
  
}

# =====================================
# 4. EOP
# =====================================
EOP.df <- run_with_retries(
  fun = function() {
    sqlQuery(con, "
      SELECT DISTINCT t1.PERSON_UID, t1.ID_NUMBER, t2.EOP_STATUS_DESCRIPTION
      FROM (
        SELECT PERSON_UID, ID_NUMBER, MAX(EOP_STATUS_DATE) AS MAX_of_EOP_STATUS_DATE
        FROM ODSMGR.EOP_BU
        WHERE EOP_STATUS != '2'
        GROUP BY PERSON_UID, ID_NUMBER
      ) t1
      INNER JOIN ODSMGR.EOP_BU t2
        ON t1.PERSON_UID = t2.PERSON_UID
       AND t1.MAX_of_EOP_STATUS_DATE = t2.EOP_STATUS_DATE
      WHERE t2.EOP_STATUS != '2'
    ", errors = FALSE)
  },
  pull_name = "EOP",
  script_loc = script_loc,
  end_loc = end_loc,
  columns = c("PERSON_UID","ID_NUMBER","EOP_STATUS_DESCRIPTION"),
  affects = "pde",
  max_tries = 3
)


if (exists("WORK.CU_GPA_EMAILS") &&
    is.data.frame(WORK.CU_GPA_EMAILS) &&
    is.data.frame(EOP.df) && nrow(EOP.df) > 0) {
  
  WORK.CU_GPA_EMAILS <- WORK.CU_GPA_EMAILS %>%
    left_join(EOP.df %>% select(PERSON_UID, EOP_STATUS_DESCRIPTION), by = "PERSON_UID") %>%
    mutate(EOP_IND = if_else(!is.na(EOP_STATUS_DESCRIPTION), "Y", "N")) %>%
    select(-EOP_STATUS_DESCRIPTION)
  
}

# =====================================
# 5. COHORT
# =====================================
COHORT.df <- run_with_retries(
  fun = function() {
    sqlQuery(con, glue("
      SELECT DISTINCT t1.PERSON_UID, t1.COHORT, t1.COHORT_DESC
      FROM ODSMGR.STUDENT_COHORT t1
      LEFT JOIN ODSMGR.STUDENT_BU t2
        ON t1.PERSON_UID = t2.PERSON_UID
       AND t1.ACADEMIC_PERIOD = t2.ACADEMIC_PERIOD
      WHERE t1.ACADEMIC_PERIOD = {academic_period}
        AND t2.PRIMARY_PROGRAM_IND = 'Y'
        AND t2.OFFICIALLY_ENROLLED = 'Y'
        AND t1.COHORT != 'EXCELRECP'
    "), errors = FALSE)
  },
  pull_name = "COHORT",
  script_loc = script_loc,
  end_loc = end_loc,
  columns = c("PERSON_UID","COHORT","COHORT_DESC"),
  affects = "pde",
  max_tries = 3
)


if (exists("WORK.CU_GPA_EMAILS") &&
    is.data.frame(WORK.CU_GPA_EMAILS) &&
    is.data.frame(COHORT.df) && nrow(COHORT.df) > 0) {
  
  WORK.CU_GPA_EMAILS_COHORT <- WORK.CU_GPA_EMAILS %>%
    left_join(COHORT.df, by = "PERSON_UID")
  
}

# =====================================
# 6. PERSON_DETAIL
# =====================================
DETAIL.df <- run_with_retries(
  fun = function() {
    sqlQuery(con, glue("
      SELECT DISTINCT t1.PERSON_UID, TO_CHAR(BIRTH_DATE, 'MM/DD/YYYY') AS DOB,
             t1.FIRST_GENERATION_IND, t1.LEGAL_SEX_DESC
      FROM ODSMGR.PERSON_DETAIL_BU t1
      LEFT JOIN ODSMGR.STUDENT_BU t2 ON t1.PERSON_UID = t2.PERSON_UID
      WHERE t2.ACADEMIC_PERIOD = {academic_period}
        AND t2.PRIMARY_PROGRAM_IND = 'Y'
        AND t2.OFFICIALLY_ENROLLED = 'Y'
    "), errors = FALSE)
  },
  pull_name = "PERSON_DETAIL",
  script_loc = script_loc,
  end_loc = end_loc,
  columns = c("PERSON_UID","DOB","FIRST_GENERATION_IND","LEGAL_SEX_DESC"),
  affects = "pde",
  max_tries = 3
)


if (exists("WORK.CU_GPA_EMAILS_COHORT") &&
    is.data.frame(WORK.CU_GPA_EMAILS_COHORT) &&
    is.data.frame(DETAIL.df) && nrow(DETAIL.df) > 0) {
  
  WORK.BIRTH <- WORK.CU_GPA_EMAILS_COHORT %>%
    left_join(DETAIL.df %>% select(PERSON_UID, DOB, FIRST_GENERATION_IND, LEGAL_SEX_DESC),
              by = "PERSON_UID")
  
}
# =====================================
# 7. RACE + ADDRESS
# =====================================
RACE.df <- run_with_retries(
  fun = function() {
    sqlQuery(con, glue("
      SELECT DISTINCT t1.PERSON_UID, t1.SUNY_RACE_ETHNICITY_CODE, t1.UNDERREPRESENTED_IND
      FROM ODSMGR.PERSON_SENSITIVE_IPEDS_BU t1
      LEFT JOIN ODSMGR.STUDENT_BU t2 ON t1.PERSON_UID = t2.PERSON_UID
      WHERE t2.ACADEMIC_PERIOD = {academic_period}
        AND t2.PRIMARY_PROGRAM_IND = 'Y'
        AND t2.OFFICIALLY_ENROLLED = 'Y'
    "), errors = FALSE)
  },
  pull_name = "RACE",
  script_loc = script_loc,
  end_loc = end_loc,
  columns = NULL,
  affects = "pde",
  max_tries = 3
)


ADDRESS.df <- run_with_retries(
  fun = function() {
    sqlQuery(con, glue("
      SELECT DISTINCT t1.ENTITY_UID AS PERSON_UID, t1.STREET_LINE1, t1.STREET_LINE2,
             t1.STREET_LINE3, t1.ADDRESS_TYPE
      FROM ODSMGR.ADDRESS t1
      LEFT JOIN ODSMGR.STUDENT_BU t2 ON t1.ENTITY_UID = t2.PERSON_UID
      WHERE t2.ACADEMIC_PERIOD = {academic_period}
        AND t1.ADDRESS_TYPE = 'CA'
        AND t2.PRIMARY_PROGRAM_IND = 'Y'
        AND t2.OFFICIALLY_ENROLLED = 'Y'
        AND t1.ADDRESS_END_DATE = DATE'2026-05-16'
    "), errors = FALSE)
  },
  pull_name = "ADDRESS",
  script_loc = script_loc,
  end_loc = end_loc,
  columns = c("PERSON_UID","STREET_LINE1","STREET_LINE2","STREET_LINE3","ADDRESS_TYPE"),
  affects = "pde",
  max_tries = 3
)


if (exists("WORK.BIRTH") &&
    is.data.frame(WORK.BIRTH) &&
    is.data.frame(RACE.df) && nrow(RACE.df) > 0 &&
    is.data.frame(ADDRESS.df) && nrow(ADDRESS.df) > 0) {
  
  ALL <- WORK.BIRTH %>%
    left_join(RACE.df, by = "PERSON_UID") %>%
    mutate(INTRNL = ifelse(SUNY_RACE_ETHNICITY_CODE == "Nonresident", "Y", "N")) %>%
    left_join(ADDRESS.df, by = "PERSON_UID") %>%
    mutate(
      HOUSING_TYPE = if_else(ADDRESS_TYPE == "CA", "On-Campus", "Off-Campus", missing = "Off-Campus"),
      COMMUNITY = if_else(ADDRESS_TYPE == "CA", STREET_LINE1, "Off-Campus", missing = "Off-Campus"),
      HALL = if_else(ADDRESS_TYPE == "CA", STREET_LINE2, "Off-Campus", missing = "Off-Campus"),
      ROOM = if_else(ADDRESS_TYPE == "CA", STREET_LINE3, "Off-Campus", missing = "Off-Campus"),
      BAP_IND = ifelse(MAJOR == "9VA", "Y", "N")
    )
  
}

# =====================================
# 8. OUTPUT
# =====================================
if (exists("ALL") && is.data.frame(ALL) && nrow(ALL) > 0) {
  
  PDE <- ALL %>%
    select(
      ID_NUMBER, FIRST_NAME, LAST_NAME, EMAIL_ADDRESS, ACADEMIC_PERIOD,
      ACADEMIC_PERIOD_DESC, STUDENT_RESIDENCY_DESC, ACADEMIC_PERIOD_ADMITTED,
      OFFICIALLY_ENROLLED, CONFIDENTIALITY_IND, DECEASED_STATUS,
      STUDENT_POPULATION_DESC, STUDENT_CLASS_DESC_BOAP, COHORT,
      COLLEGE_DESC, MAJOR, MAJOR_DESC, DEPARTMENT_DESC, BAP_IND, EOP_IND,
      FIRST_GENERATION_IND, INTRNL, LEGAL_SEX_DESC, GENDER_IDENTITY_DESC,
      DOB, SUNY_RACE_ETHNICITY_CODE, UNDERREPRESENTED_IND,
      ACADEMIC_STUDY_VALUE, CU_GPA, HOUSING_TYPE, COMMUNITY, HALL, ROOM
    )
  
  write_xlsx(PDE, dump_path_xlsx)
  saveRDS(PDE, dump_path_rds)
  
  pin_write(board, PDE, name = "PDE_Pin")
  
}
# ---- Save Pull Results ----
PR_PATH <- get_data_path("Data Hub/Data_Pull_Overview_App/Data/pull_results/PDE_PR.rds")
dir.create(dirname(PR_PATH), recursive = TRUE, showWarnings = FALSE)
saveRDS(pull_results, PR_PATH)

all_success <- pull_results %>%
  summarise(all_ok = all(success)) %>%
  pull(all_ok)

if (!all_success) {
  
  failed <- pull_results %>% filter(!success)
  
  error_table <- failed %>%
    transmute(
      Pull = pull_name,
      Error = ifelse(lengths(error_msg) == 0, "Unknown error", error_msg)
    )
  
  error_html <- error_table %>%
    kableExtra::kbl("html") %>%
    kableExtra::kable_styling(full_width = FALSE) %>%
    as.character()
  
  error_email <- compose_email(
    body = html(paste0(
      "<p>❌ <strong>PDE export failed on ", Sys.Date(), "</strong></p>",
      "<p><strong>Expected output:</strong><br>", pin_path, "</p>",
      "<p><strong>Failed steps:</strong></p>",
      error_html
    )),
    footer = "— Automated PDE Script"
  )
  
  smtp_send(
    error_email,
    from = "mjacob28@binghamton.edu",
    to   = c("mjacob28@binghamton.edu","ewalsh@binghamton.edu", "bshabroski@binghamton.edu", "assess@binghamton.edu"),
    subject = paste("❌ PDE SPRING 2026 Export Failed:", Sys.Date()),
    credentials = creds_envvar(
      host = Sys.getenv("SMTP_SERVER"),
      user = Sys.getenv("SMTP_USER"),
      pass_envvar = "SMTP_PASS",
      port = 465,
      use_ssl = TRUE
    )
  )
  
  stop("PDE pull failed — see pull_results")
}

summary_df <- PDE %>%
  count(STUDENT_POPULATION_DESC, name = "Count") %>%
  mutate(Percent = round(Count / sum(Count) * 100, 1)) %>%
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
  rename(Population = STUDENT_POPULATION_DESC) %>%
  kable("html", escape = FALSE, align = "lrr", caption = "Student Population Breakdown") %>%
  kable_styling("striped", full_width = FALSE)

email <- compose_email(
  body = html(paste0(
    "<p>✅ PDE export completed successfully on ", Sys.Date(), ".</p>",
    "<p><strong>Pin updated:</strong><br>", pin_path, "</p>",
    summary_html,
    "<p><strong>View Full Shiny App:</strong><br>Z:/Data Hub/PDE_APP_SP2026</p>"
  )),
  footer = "— Automated PDE Script"
)

smtp_send(
  email,
  from = "mjacob28@binghamton.edu",
  to   = c("mjacob28@binghamton.edu", "ewalsh@binghamton.edu","bshabroski@binghamton.edu", "assess@binghamton.edu"),
  subject = paste("✅ PDE SPRING 2026 Export Success:", Sys.Date()),
  credentials = creds_envvar(
    host = Sys.getenv("SMTP_SERVER"),
    user = Sys.getenv("SMTP_USER"),
    pass_envvar = "SMTP_PASS",
    port = 465,
    use_ssl = TRUE
  )
)


# ---- Cleanup ----
close(con)
dbDisconnect(conn)
