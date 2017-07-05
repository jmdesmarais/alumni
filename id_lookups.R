#install.packages(c(RODBC, reshape2, stringdist, dplyr, plyr))

library(RODBC) #Connect and query to SQL Server DB's
library(reshape2) #For dataframe casting and melting
library(stringdist) #Compute similarity measure between strings
library(plyr)
library(dplyr)


# Input directories and file name
resultsDirectory <- "K:\\Ard\\Alumni Relations\\Data Management\\Non-OAR Alumni Engagement\\id_matching\\2017"
wd <- "K:\\Ard\\Alumni Relations\\Data Management\\Non-OAR Alumni Engagement\\2017\\id_matching"
fileName <- "allAttendees"
resultFileName <- "id_matches_q1_events"
allDegreesExtract <- "AndersonAllDegreeandRevenueExtract_20170609.csv"

setwd(wd)  
#columns are Event.Type, Event.Name, Event.Date, Event.Location, Host.Group, Contact, CRM.ID, 
# First.Name, Last.Name, Year, Program, Email, Registration.Type, Attended, Type.of.Engagement. Comment
found <- read.csv(paste0(fileName, ".csv"), na.strings =c("", "#NA", "<N/A>", "<NA>", "N/A"), 
                                            strip.white=TRUE, 
                                            stringsAsFactors=FALSE)

found$Program <- as.factor(toupper(found$Program))
found$Year <- as.integer(found$Year)
found$Match.Similarity <- NA

table(is.na(found$CRM.ID))
# Split into unfound and not unfound
unfound<- found[is.na(found['CRM.ID']),]
found <- found[!is.na(found['CRM.ID']),]
# Add column to record method for matching ID
unfound$Match.Method = "Not Matched"
found <- dplyr::mutate(found, Match.Method="Given")

#########################################
## GETTING REFERENCE DATA FROM TOAD & CRM #####
###################################
#Getting the data 
##Get email name and degree data from CRM using All Degrees file
wd <- "K:\\Ard\\Alumni Relations\\Data Management\\DPR\\Updated All Degrees File"
setwd(wd)
ecrm <- read.csv(allDegreesExtract, na.strings =c("", "#NA", "<N/A>", "<NA>", "N/A"), strip.white=TRUE)
alumCols <- c("CONSTITUENTLOOKUPID", "FIRSTNAME", "LAST_NAME",
              "DEGREE1_YEAR", "DEGREE1_CODE","DEGREE1_MAJOR_CODE",
              "HOME_EMAILADDRESS", "BUSINESS_EMAILADDRESS", "Program")
ecrm <- ecrm[alumCols]

#Get degree and email and preferred name from TOAD
##Create connection to Anderson's SQL server
dbhandle <- odbcDriverConnect('driver={SQL Server};
                              server=sqldb.anderson.ucla.edu;
                              database=alumni;
                              trusted_connection=true')
##Use connection to select all active emails and corresponding CRM ID's 
sqlString <- "SELECT a.id,a.fname, a.mname, a.lname, a.preferred_name, d.type, d.email
              FROM alum_agsmdata AS a JOIN alum_address AS d 
                ON a.id = d.id JOIN alum_edu e ON e.id = a.id
              WHERE d.status ='A'"

toad_email <- unique(sqlQuery(dbhandle, sqlString))

##Reshape data to fit All Degrees file format
toad_email <- dcast(toad_email, id + fname + mname + lname + preferred_name ~ type, value.var = "email")
##Get all emails from email log
sqlString <- "SELECT ISNULL(l.id, lef.advance_id) AS id, lef.correction_data,l.old_email, l.new_email
              FROM alum_email_log l FULL OUTER JOIN
                  (SELECT s.advance_id, s.correction_data
                  FROM student_data_corrections s
                  WHERE s.correction_field LIKE 'email%') lef
              ON lef.advance_id = l.id"

all_emails <- unique(sqlQuery(dbhandle, sqlString))
all_emails$crm_id <- as.integer(as.character(all_emails$id))

##Merge TOAD and CRM dataframes using alumni ID's
alumni <- merge(toad_email, ecrm, by.x = "id", by.y="CONSTITUENTLOOKUPID", all.y=TRUE)
##Rename columns makes reading algorithm easier
alumni <- plyr::rename(alumni, c("HOME_EMAILADDRESS" = "crm_h_email", "LAST_NAME" = "crm_last_name",
                                "FIRSTNAME" = "crm_first_name", "BUSINESS_EMAILADDRESS" = "crm_b_email",
                                "B" = "toad_b_email", "H"="toad_h_email", "P"="toad_p_email",
                                "fname" = "toad_first_name", "lname" = "toad_last_name", "mname" = "toad_middle_name"))

##Lowercase all text columns 
charCols <- c("toad_first_name", "toad_last_name", "toad_middle_name", "preferred_name", 
              "toad_b_email", "toad_h_email", "toad_p_email", "crm_h_email", "crm_b_email",
              "crm_last_name", "crm_first_name")
alumni[,charCols] <- lapply(alumni[,charCols], tolower)
charCols <- c("First.Name", "Last.Name", "Email")
unfound[,charCols] <- lapply(unfound[,charCols], tolower)
alumni <- transform(alumni, toad_last_name = colsplit(toad_last_name, pattern = " ", names = c('a', 'b', 'c', 'd', 'e')))
alumni$toad_last_name_full <- gsub(" ", "", paste0(alumni$toad_last_name$a, alumni$toad_last_name$b, alumni$toad_last_name$c, 
                                                   alumni$toad_last_name$d, alumni$toad_last_name$e))
##Create Anderson Email account for each alumni's by concatenating firstName.lastName.degreeYear@anderson.ucla.edu
alumni$anderson_email <-  gsub(" ", "", tolower(paste0(alumni$toad_first_name, ".", alumni$toad_last_name$a, alumni$toad_last_name$b, 
                               alumni$toad_last_name$c, alumni$toad_last_name$d, alumni$toad_last_name$e,
                               alumni$toad_last_name$f,".",alumni$DEGREE1_YEAR, "@anderson.ucla.edu")))
alumni$anderson_email2 <- tolower(paste0(alumni$toad_first_name, ".", alumni$toad_last_name$a, 
                                        ".",alumni$DEGREE1_YEAR, "@anderson.ucla.edu"))
alumni$anderson_email3 <- gsub(" ", "", tolower(paste0(alumni$toad_first_name, ".", alumni$toad_last_name$a, alumni$toad_last_name$b, 
                                                       alumni$toad_last_name$c,".",alumni$DEGREE1_YEAR, "@anderson.ucla.edu")))

alumni$anderson_email4 <-  gsub(" ", "", tolower(paste0(alumni$toad_first_name, ".", alumni$toad_last_name$a, alumni$toad_last_name$b, 
                                                       alumni$toad_last_name$c, alumni$toad_last_name$d,
                                                       ".",alumni$DEGREE1_YEAR, "@anderson.ucla.edu")))
alumni$anderson_email5 <- tolower(paste0(alumni$preferred_name, ".", alumni$toad_last_name$a, ".",
                                         alumni$DEGREE1_YEAR, "@anderson.ucla.edu"))
#Matching Alumni ID's 

##Match on email -- for every email provided check all TOAD and CRM emails. Only use unique matches
records <- dim(unfound)[[1]]
for(i in seq(dim(unfound)[[1]])){
  if(i%%100 == 0){
    print("")
    searched <- round(i/records,3)*100
    f <- round(table(is.na(unfound$CRM.ID))[1]/records, 3)*100
    print(paste0(searched, "% of records searched, ", f, "% Total ID's located"))
  }
   email <- as.character(unfound[i, "Email"])
   m <- subset(alumni, toad_b_email == email  | toad_h_email == email | toad_p_email == email |
               crm_h_email == email | crm_b_email == email)
  if(dim(m)[1]== 1){
      id <- m[['id']]
      unfound[i,'CRM.ID'] = id
      unfound[i, 'Match.Method'] = 'Email Matched - Current'
    next
  }
  m <- unique(subset(all_emails, old_email == email | new_email == email)[,c("id", "crm_id")])
  if(dim(m)[1] == 1){
    if(m[['crm_id']] %in% alumni[['id']]){
      id <- m[['crm_id']]
      unfound[i,'CRM.ID'] = id
      unfound[i, 'Match.Method'] = 'Email Matched - past email'
      next
    }
  }
  m <- subset(alumni, anderson_email == email | anderson_email2 == email |
                anderson_email3 == email | anderson_email4 == email | anderson_email5 == email)
  if(dim(m)[1]==1){
    if(m[['id']] %in% alumni[['id']]){
      id <- m[['id']]
      unfound[i,'CRM.ID'] = id
      unfound[i, 'Match.Method'] = 'Email Matched - LEF'
    next
      }
  }
  m <- subset(all_emails, correction_data == email)
  if(dim(m)[1] ==1){
    if(m[['id']] %in% alumni[['id']]){
      id <- m[['id']]
      unfound[i,'CRM.ID'] = id
      unfound[i, 'Match.Method'] = 'Email Matched - LEF student corrections'
      next
    }
  }
  }

##Check how many ID's found
table(is.na(unfound$CRM.ID))
##Remove found records and append to found
found <- unfound %>%
                  filter(!is.na(CRM.ID)) %>%
                  bind_rows(found)

unfound <- filter(unfound, is.na(CRM.ID))

##Match on Name, Program, and Graduation Year
records <- dim(unfound)[[1]]
for(i in seq(dim(unfound)[[1]])){
  if(i%%100 == 0){
    print("")
    searched <- round(i/records,3)*100
    f <- round(table(is.na(unfound$CRM.ID))[1]/records, 3)*100
    print(paste0(searched, "% of records searched, ", f, "% Total ID's located"))
  }
  fname <- unfound[i,"First.Name"]
  lname <- unfound[i, "Last.Name"]
  lname2 <-""
  #Check for multiple last names
  if(!is.na(strsplit(lname, " ")[[1]][2])){
    f <-strsplit(lname, " ")[[1]][1]
    lname2 <-strsplit(lname, " ")[[1]][2]
    lname <- f
  }
  year <- unfound[i, "Year"]
  if(is.na(year)){year<-""}
  program <- as.character(unfound[i, "Program"])
  if(is.na(program)){program <- ""}
  #Last Name Exact Match and Preferred or First Name Exact Match
  if(lname2==""){m <- subset(alumni, (toad_last_name$a == lname | toad_last_name$b == lname | crm_last_name == lname ) & 
                                (crm_first_name ==fname | preferred_name == fname))}
  else{m <- subset(alumni, (crm_last_name == lname | crm_last_name == lname2 | toad_last_name$a %in% c(lname, lname2) | 
                              toad_last_name$b %in% c(lname, lname2)) & 
                      (toad_first_name == fname | crm_first_name== fname))}
  
  m2 <- subset(m, Program == program | DEGREE1_YEAR == year)
  if(dim(m2)[1]==1){
    if(m2[['Program']] == program & m2[['DEGREE1_YEAR']] == year){
      unfound[i,'CRM.ID'] = m2[['id']]
      unfound[i, 'Match.Method'] = 'First Name, Last Name Program Year'
      next
    }
    if(m2[['Program']] == program){
      unfound[i,'CRM.ID'] = m2[['id']]
      unfound[i, 'Match.Method'] = 'First Name, Last Name Program'
      next
    }
    if(m2[['DEGREE1_YEAR']] == year){
      unfound[i,'CRM.ID'] = m2[['id']]
      unfound[i, 'Match.Method'] = 'First Name, Last Name Year'
      next
    }
  }
  else{
     if(dim(m2)[1]>1){
      if(dim(m2[m2$DEGREE1_YEAR==year,])[1]==1){
        m2 <- m2[m2$DEGREE1_YEAR == year,]
        unfound[i,'CRM.ID'] = m2[['id']]
        unfound[i, 'Match.Method'] = 'First Name, Last Name Year'
        next
      }
     }
    }
  m<- subset(alumni, grepl(lname, crm_last_name) & 
               (grepl(fname, crm_first_name) |  grepl(fname, preferred_name)))
  if(dim(m)[1]==1){
    if(m[['Program']] == program){
      unfound[i,'CRM.ID'] = m[['id']]
      unfound[i, 'Match.Method'] = 'First Name Contains Last Name Contains Program'
      next 
    }
    if(m[['DEGREE1_YEAR']] == year){
      unfound[i,'CRM.ID'] = m[['id']]
      unfound[i, 'Match.Method'] = 'First Name, Last Name Year'
      next 
    }
  }
}
  
table(is.na(unfound$CRM.ID))
##Remove found records and append to found
found <- unfound %>%
                  filter(!is.na(CRM.ID)) %>%
                  bind_rows(found)
unfound <- filter(unfound, is.na(CRM.ID))

table(found$Match.Method)
#Add match similarity measure for all matched ID's and the name of the constituent they were matched to
for(i in seq(dim(found)[[1]])){
    id <- found[i,'CRM.ID']
    attendeeName <- paste0(found[i,'First.Name'], found[i,'Last.Name'])
    formalName <- paste0(alumni[alumni$id==id, 'toad_first_name'], alumni[alumni$id == id, 'toad_last_name'])
    formalName2 <- paste0(alumni[alumni$id==id, 'crm_first_name'], alumni[alumni$id == id, 'crm_last_name'])
    informalName <- paste0(alumni[alumni$id==id, 'preferred_name'], alumni[alumni$id == id, 'toad_last_name'])
    informalName2 <- paste0(alumni[alumni$id==id, 'preferred_name'], alumni[alumni$id==id, 'crm_last_name'])
    middleLast <- paste0(alumni[alumni$id == id, 'toad_middle_name'], alumni[alumni$id==id, 'crm_last_name'])
    
    sim <- max(1-stringdist(formalName,attendeeName, method='jaccard'),
               1-stringdist(formalName2,attendeeName, method='jaccard'),
               1-stringdist(informalName,attendeeName, method='jaccard'),
               1-stringdist(informalName2,attendeeName, method='jaccard'),
               1-stringdist(middleLast, attendeeName, method = 'jaccard'))
    
    found[i,'Match.Similarity'] = sim
  }




setwd(resultsDirectory)
write.csv(found, paste0(resultFileName,"v",Sys.Date() , ".csv"), row.names=FALSE)
write.csv(unfound, paste0('unable_to_match_v',Sys.Date(), '.csv'), row.names=FALSE)






