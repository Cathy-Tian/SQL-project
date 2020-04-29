# load library
library(tidyr)
library(rjson)
library(RPostgreSQL)
# load the postgresql driver
drv = dbDriver("PostgreSQL")
# create a connection
con = dbConnect(drv,
                host='cu-spring2020-group3.cggz75b61mlh.us-east-2.rds.amazonaws.com',
                dbname = "postgres",
                user = "postgres",
                password = "postgres",
                port = 5432)



############# keyword & movie keyword
df=read.csv('movie.csv',stringsAsFactors = F)
new_df=data.frame(matrix(ncol=3,nrow=0))
for (i in seq(nrow(df))){
  json_list=fromJSON(df$keywords[i])
  if (length(json_list)==0){
    next
  }
  for (j in seq(length(json_list))){
    temp1=as.data.frame(json_list[j])
    new_df=rbind(
      new_df,setNames(data.frame(df$id[i],temp1$id,as.character(temp1$name)),names(new_df))
    )
  }
}
unique_df=new_df[!duplicated(new_df),]
unique_df=unique_df[!is.na(unique_df$X3),]
keyword=subset(unique_df,select=c('X2','X3'))
colnames(keyword)=c('keyword_id','keyword')
keyword=unique(keyword)
dbWriteTable(con,'keyword', keyword, row.names=FALSE, append=T)
movie_keyword=unique(subset(unique_df,select=c('X1','X2')))
colnames(movie_keyword)=c('movie_id','keyword_id')
dbWriteTable(con,'movie_keyword',movie_keyword,row.names=F,append=T)


############# genre & movie genre
df = read.csv('/Users/wuyue/Desktop/sql/movie.csv')
# Split genres
genre_df = unique(df[c('id','genres')])
s_genre <- strsplit(as.character(genre_df$genres), split = "},", fixed=TRUE)
# Create a new expanded dataframe
data_genre <- data.frame(movie_id = rep(genre_df$id, sapply(s_genre, length)),
                            genres = unlist(s_genre))
# split the id and name
data_genre_1 = separate(data_genre, 'genres', paste("genres", 1:3, sep="_"), sep=":", extra="drop")
genre=data_genre_1[c('movie_id','genres_3')]
#clean the text by removing punctuation
library(tm)
# clean language_id
corpus1 = Corpus(VectorSource(genre$genres_3))
corpus1 = tm_map(corpus1,FUN = removePunctuation)
genre_type=data.frame(genre_type = sapply(corpus1, as.character), stringsAsFactors = FALSE)
#genre_clean
genre_clean=cbind(genre[c('movie_id')],genre_type[c('genre_type')])
#create genre table
genre_entity = unique(genre_clean['genre_type'])
# Add incrementing integers for genre_id
genre_entity$genre_id <- 1:nrow(genre_entity)
#push the genre data into genre table
dbWriteTable(con, name="genre", value=genre_entity, row.names=FALSE, append=TRUE)
#movie_genre relationship
# Map genre_id
genre_id_list <- sapply(genre_clean$genre_type, function(x) genre_entity$genre_id[genre_entity$genre_type == x])
# Add genre_id to the main dataframe
genre_clean$genre_id <- genre_id_list
#create movie_genre relationship
movie_genre <- genre_clean[c('movie_id', 'genre_id')]
##push the genre data into genre table
dbWriteTable(con, name="movie_genre", value=movie_genre, row.names=FALSE, append=TRUE)


############# movie
setwd=('/Users/windywen/Downloads')
data = read.csv('movie.csv')
spoken_language_df_entity = read.csv('spoken_language_df_entity.csv')
spoken_language_df_relationship = read.csv('spoken_language_df_relationship.csv')
movie_df = unique(data[c('id','original_title','release_date','status','original_language','revenue','budget','runtime','overview')])
# rename the id as movie_id
names(movie_df)[names(movie_df)=='id']='movie_id'
# push movie table
dbWriteTable(con, name="movie", value=movie_df, row.names=FALSE, append=TRUE)


############# rating_imdb
rating_imdb_df = unique(data[c('id','vote_average','vote_count')])
#rename the id as movie_id/ vote as rating
names(rating_imdb_df)[names(rating_imdb_df)=='id']='movie_id'
names(rating_imdb_df)[names(rating_imdb_df)=='vote_average']='rating'
names(rating_imdb_df)[names(rating_imdb_df)=='vote_count']='rating_count'
#push the movie data
dbWriteTable(con, name="rating_imdb", value=rating_imdb_df, row.names=FALSE, append=TRUE)


############ production_company & movie company
setwd('C:/Users/chent/Desktop/5310')
df=read.csv('movie.csv',stringsAsFactors = F,encoding = 'utf8')
new_df=data.frame(matrix(ncol=3,nrow=0))
for (i in seq(nrow(df))){
  json_list=fromJSON(df$production_companies[i])
  if (length(json_list)==0){
    next
  }
  # print(i)
    for (j in seq(length(json_list))){
      temp1=as.data.frame(json_list[j])
      new_df=rbind(
        new_df,setNames(data.frame(df$id[i],temp1$id, as.character(temp1$name)),names(new_df))
      )
    }
}
new_df
#delete multiple
unique_df=new_df[!duplicated(new_df),]
unique_df=unique_df[!is.na(unique_df$X3),]
production_companies=subset(unique_df,select=c('X2','X3'))
production_companies
colnames(production_companies)=c('id','name')
production_companies=unique(production_companies)
#create table
dbWriteTable(con,'production_companies', production_companies, row.names=FALSE, append=T)
# working on moive_companies
movie_companies=unique(subset(unique_df,select=c('X1','X2')))
colnames(movie_companies)=c('movie_id','companies_id')
dbWriteTable(con,'movie_companies', movie_companies, row.names=FALSE, append=T)


############ ceremony & award & nominee & acmn
install.packages("splitstackshape", dep=T)
#Read Data
df <- read.csv('movie.csv')
head(df)
#Clean Year column
#Transform - two years un a couple rows
df <- cSplit(df, "Year", "/")
#Assumption: First Year is the year the award ceremony is scored off of. Deleted second year.
df <- subset(df, select = -c(Year_2))
names(df)[names(df)=="Year_1"] <- "Year"
#Assumption "ceremony" data type will represent the ceremony_id and it is distinct and unique
ceremony_df <- data.frame('ceremony_id' = df$Ceremony, 'year' = df$Year)
ceremony_df <- unique(ceremony_df)
dbWriteTable(EEcon, name="ceremony", value=ceremony_df, row.names=FALSE, append=TRUE)
#Award Table
#Create IDs
temp_awardtype_df <- data.frame('award' = unique(df$Award))
temp_awardtype_df$award_id <- 1:nrow(temp_awardtype_df)
#add award_id to original df and change "Award" to "award_type"
awardtype_list <- sapply(df$Award, function(x) temp_awardtype_df$award_id[temp_awardtype_df$award == x])
df$award_id <- awardtype_list
names(df)[names(df)=="Award"] <- "award_type"
#put award data into award tables
award_df <- data.frame('award_id' = df$award_id, 'award_type' = df$award_type)
award_df <- unique(award_df)
dbWriteTable(EEcon, name="award", value=award_df, row.names=FALSE, append=TRUE)
#nominees table
temp_nominee_df <- data.frame('nominee' = unique(df$Detail))
temp_nominee_df$nominee_id <- 1:nrow(temp_nominee_df)
#add nominee_id to original df and change "Detail" to "nominee"
nominee_list <- sapply(df$Detail, function(x) temp_nominee_df$nominee_id[temp_nominee_df$nominee == x])
df$nominee_id <- nominee_list
names(df)[names(df)=="Detail"] <- "nominee"
#put nominee data into nominee table
nominee_df <- data.frame('nominee_id' = df$nominee_id, 'nominee_name' = df$nominee, 'nominee_type' = df$detail.type)
nominee_df <- unique(nominee_df)
dbWriteTable(EEcon, name="nominee", value=nominee_df, row.names=FALSE, append=TRUE)
#acmn table
#pulling movie data changes to my own workspace
names(df)[names(df)=='id']='movie_id'
temp_acmn <- data.frame('award_id' = df$award_id, 'ceremony_id' = df$Ceremony, 'movie_id' = df$movie_id, 'nominee_id' = df$nominee_id, 'win_or_not' = df$Winner)
temp_acmn <- unique(temp_acmn)
dbWriteTable(EEcon, name="acmn", value=temp_acmn, row.names=FALSE, append=TRUE)


############# spoken language and movie language
require('tm')
require('tidyr')
setwd=('/Users/windywen/Downloads')
data = read.csv('movie.csv')
# Split language
spoken_language_df = unique(data[c('id','spoken_languages')])
s_language <- strsplit(as.character(spoken_language_df$spoken_languages), split = "},", fixed=TRUE)
# Create a new expanded dataframe
data_language <- data.frame(movie_id = rep(spoken_language_df$id, sapply(s_language, length)),
                            spoken_languages = unlist(s_language))
#split the id and name
data_language_1 = separate(data_language, 'spoken_languages', paste("spoken_languages", 1:3, sep="_"), sep=":", extra="drop")
data_language_2 = separate(data_language_1, 'spoken_languages_2', paste("spoken_languages", 4:5, sep="_"), sep=",", extra="drop")
spoken_language=data_language_2[c('movie_id','spoken_languages_4','spoken_languages_3')]
#clean the text by removing punctuation
request(tm)
#clean language_id
corpus1 = Corpus(VectorSource(spoken_language$spoken_languages_4))
corpus1 = tm_map(corpus1,FUN = removePunctuation)
language_id=data.frame(language_id = sapply(corpus1, as.character), stringsAsFactors = FALSE)
#clean language_name
corpus2 = Corpus(VectorSource(spoken_language$spoken_languages_3))
corpus2 = tm_map(corpus2,FUN = removePunctuation)
language_name=data.frame(language_name = sapply(corpus2, as.character), stringsAsFactors = FALSE)
#cleaned dataframe
spoken_language_clean=cbind(spoken_language[c('movie_id')],language_id[c('language_id')],language_name[c('language_name')])
#clean space
searchString <- ' '
replacementString <- ''
spoken_language_clean$language_name = gsub(searchString,replacementString,spoken_language_clean$language_name)
spoken_language_clean$language_id = gsub(searchString,replacementString,spoken_language_clean$language_id)
#clean srange value of language_name
spoken_language_clean$language_name[spoken_language_clean$language_name=='Espau00f1ol']='Spanish'
spoken_language_clean$language_name[spoken_language_clean$language_name=='Franu00e7ais']='Franch'
spoken_language_clean$language_name[spoken_language_clean$language_name=='Portuguu00eas']='Portuguese'
spoken_language_clean$language_name[spoken_language_clean$language_name=='Pu0443u0441u0441u043au0438u0439']='Russian'
spoken_language_clean$language_name[spoken_language_clean$language_name=='Tiu1ebfngViu1ec7t']='Vietnamese'
spoken_language_clean$language_name[spoken_language_clean$language_name=='Tu00fcrku00e7e']='Turkish'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u00cdslenska']='Icelandic'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u010cesku00fd']='Czech'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u03b5u03bbu03bbu03b7u03bdu03b9u03bau03ac']='Greek'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u0423u043au0440u0430u0457u043du0441u044cu043au0438u0439']='Ukrainian'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u049bu0430u0437u0430u049b']='Kazakh'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u05e2u05b4u05d1u05b0u05e8u05b4u05d9u05ea']='Hebrew'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u0627u0631u062fu0648']='Urdu'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u0627u0644u0639u0631u0628u064au0629']='Arabic'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u0641u0627u0631u0633u06cc']='Persian'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u067eu069au062au0648']='Pashto'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u0939u093fu0928u094du0926u0940']='Hindi'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u0ba4u0baeu0bbfu0bb4u0bcd']='Tamil'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u0e20u0e32u0e29u0e32u0e44u0e17u0e22']='Thai'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u5e7fu5ddeu8bddu5ee3u5ddeu8a71']='Chinese'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u65e5u672cu8a9e']='Japanese'
spoken_language_clean$language_name[spoken_language_clean$language_name=='u666eu901au8bdd']='Chinese'
spoken_language_clean$language_name[spoken_language_clean$language_name=='ud55cuad6duc5b4uc870uc120ub9d0']='Korean'
#unique df
spoken_language_df = unique(spoken_language_clean[c('movie_id','language_id','language_name')])
#full fill the language_name
spoken_language_df$language_name[is.na(spoken_language_df$language_id)]='NoLanguage'
spoken_language_df$language_id[is.na(spoken_language_df$language_id)]='xx'
spoken_language_df$language_name[spoken_language_df$language_id=='yi']='Yiddish'
spoken_language_df$language_name[spoken_language_df$language_id=='km']='Khmer'
spoken_language_df$language_name[spoken_language_df$language_id=='mi']='Maori'
spoken_language_df$language_name[spoken_language_df$language_id=='sh']='Serbo-Croatian'
spoken_language_df$language_name[spoken_language_df$language_id=='hy']='Armenian'
spoken_language_df$language_name[spoken_language_df$language_id=='ne']='Nepali'
spoken_language_df$language_name[spoken_language_df$language_id=='si']='Sinhala'
spoken_language_df$language_name[spoken_language_df$language_id=='kw']='Cornish'
spoken_language_df$language_name[spoken_language_df$language_id=='gd']='Scottish Gaelic'
spoken_language_df$language_name[spoken_language_df$language_id=='bo']='Tibetan'
spoken_language_df$language_name[spoken_language_df$language_id=='co']='Corsican'
spoken_language_df$language_name[spoken_language_df$language_id=='xh']='Xhosa'
spoken_language_df$language_name[spoken_language_df$language_id=='ny']='Chewa'
spoken_language_df$language_name[spoken_language_df$language_id=='st']='Southern Sotho'
#table spoken_language
spoken_language_df_entity = unique(spoken_language_df[c('language_id','language_name')])
#table movie_spoken_language
spoken_language_df_relationship = unique(spoken_language_df[c('movie_id','language_id')])
#save the local files
write.csv(spoken_language_df_entity, 'spoken_language_df_entity.csv',row.names =F)
write.csv(spoken_language_df_relationship, 'spoken_language_df_relationship.csv',row.names =F)
# read data
spoken_language_df_entity = read.csv('spoken_language_df_entity.csv')
spoken_language_df_relationship = read.csv('spoken_language_df_relationship.csv')
#push the spoken_language data
dbWriteTable(con, name="spoken_language", value=spoken_language_df_entity, row.names=FALSE, append=TRUE)
#push the spoken_language data
dbWriteTable(con, name="movie_spoken_language", value=spoken_language_df_relationship, row.names=FALSE, append=TRUE)


############# production country
setwd('/Users/cathy/Downloads')
data = read.csv('movie.csv', stringsAsFactors = F)
# create dataframe with only id and production country
temp_country_df = data.frame(data[c('id','production_countries')])
# delete rows with missing values
temp_country_df = temp_country_df[order(temp_country_df$production_countries), ]
temp_country_df = temp_country_df[17:nrow(temp_country_df), ]
# split the contents by comma:
x <- strsplit(as.character(temp_country_df$production_countries), ", ", fixed = T)
# add new rows with each content:
new_data <- cbind(temp_country_df[rep(1:nrow(temp_country_df), lengths(x)), 1:2], content = unlist(x))
# extract and add the code:
new_data$code <- sub(".*\\((\\d+)\\s.*", "\\1", new_data$production_countries)
new_data = new_data[c('id', 'content')]
# delete unnecessary rows
toDelete <- seq(2, nrow(new_data), 2)
new_data = new_data[ toDelete ,]
# rename column
colnames(new_data)[2] <-"country"
# repeat above procedure
y <- strsplit(as.character(new_data$country), ": ", fixed = T)
new_data <- cbind(new_data[rep(1:nrow(new_data), lengths(y)), 1:2], content = unlist(y))
new_data$code <- sub(".*\\((\\d+)\\s.*", "\\1", new_data$country)
new_data = new_data[c('id', 'content')]
toDelete <- seq(2, nrow(new_data), 2)
new_data = new_data[ toDelete ,]
colnames(new_data)[2] <-"country"
# delete punctuations
library(tm)
corpus = Corpus(VectorSource(new_data$country))
corpus = tm_map(corpus,FUN = removePunctuation)
country = data.frame(country = sapply(corpus, as.character), stringsAsFactors = FALSE)
new_data$country = country
# add value to production country table
production_country = data.frame('country_name' = unique(new_data$country))
production_country$country_id = 1:nrow(production_country)
write.csv(production_country,"/Users/cathy/Downloads/production_country.csv", row.names = FALSE)


############# movie country
setwd('/Users/cathy/Downloads')
production = read.csv('production_country.csv', stringsAsFactors = F)
df = data.frame(production)
data = read.csv('movie.csv', stringsAsFactors = F)
data = data[order(data$production_countries), ]
data = data[17:nrow(data), ]
# split by comma in production_countries column
z <- strsplit(as.character(data$production_countries), ", ", fixed = T)
# add new rows with each content:
data1 = cbind(data[rep(1:nrow(data), lengths(z)), 1:25], content = unlist(z))
# extract and add the code
data1$code <- sub(".*\\((\\d+)\\s.*", "\\1", data1$production_countries)
# delete unnecessary rows and columns
data1 = data1[, 1:26]
toDelete <- seq(2, nrow(data1), 2)
data1 = data1[ toDelete ,]
colnames(data1)[26] <-"country_name"
# repeat the above procedure
w <- strsplit(as.character(data1$country_name), ": ", fixed = T)
data1 <- cbind(data1[rep(1:nrow(data1), lengths(w)), 1:26], content = unlist(w))
data1$code <- sub(".*\\((\\d+)\\s.*", "\\1", data1$country_name)
toDelete <- seq(2, nrow(data1), 2)
data1 = data1[ toDelete ,]
colnames(data1)[27] <-"country"
# delete punctuation
library(tm)
corpus = Corpus(VectorSource(data1$country))
corpus = tm_map(corpus,FUN = removePunctuation)
country = sapply(corpus, as.character)
data1$country = country
data1$country = as.character(data1$country)
# append country id to original dataset
country_id_list <- sapply(data1$country, function(x) df$country_id[df$country == x])
data1$country_id <- country_id_list
raw_movie_country = data.frame(data1[c(10, 29)])
write.csv(raw_movie_country,"/Users/cathy/Downloads/raw_movie_country.csv", row.names = FALSE)
# read data
data = read.csv('raw_movie_country.csv')
production_country = read.csv('production_country.csv')
# change column name
colnames(production_country)[1] = 'country_name'
production_country$country_name = as.character(production_country$country_name)
# push data
dbWriteTable(con, name="production_country", value=production_country, row.names=FALSE, append=TRUE)
colnames(data)[1] <-"movie_id"
colnames(data)[2] <-"country_id"
dbWriteTable(con, name='movie_country', value=data[c('movie_id', 'country_id')]
             [!duplicated(data[c('movie_id', 'country_id')]), ], row.names=FALSE, append=TRUE)





# disconnect
dbDisconnect(con)
closeAllConnections()
