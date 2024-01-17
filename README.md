# 311-DataPipeline
a pipeline that connects
to SeeClickFix and downloads all the issues for a city, and then loads it in Elasticsearch.
I am currently running this pipeline every 8 hours. I use this pipeline as a source of open
source intelligence – using it to monitor quality of life issues in neighborhoods, as well as
reports of abandoned vehicles, graffiti, and needles. 