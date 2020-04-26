while read i
do
	PYTHONPATH='.' AWS_PROFILE=omar luigi --module cleaned_ingest cleaned_task --year $1 --month $2 --station $i --local-scheduler
done < ./estaciones.txt