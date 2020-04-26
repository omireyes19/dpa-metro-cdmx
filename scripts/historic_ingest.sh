while read i
do
	for j in $(seq 2018 2019)
	do
		for k in $(seq 1 12)
		do
			PYTHONPATH='.' AWS_PROFILE=omar luigi --module cleaned_ingest cleaned_task --year $j --month $k --station $i --local-scheduler
		done
	done
done < ./estaciones.txt