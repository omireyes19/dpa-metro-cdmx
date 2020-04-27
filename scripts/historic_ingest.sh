for i in $(seq 2018 2019)
do
	for j in $(seq 1 12)
	do
		while read k
		do
			PYTHONPATH='.' AWS_PROFILE=omar luigi --module cleaned_ingest cleaned_task --year $i --month $j --station $k --local-scheduler
		done
	done
done < ./estaciones.txt