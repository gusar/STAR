#!/usr/bin/env bash
mongoimport -d star_raw -c raw_staging --file stocktwits_messages_dec_2014.json > mongoimport_stocktwits_messages_dec_2014.out
mongoimport -d star_raw -c raw_staging --file stocktwits_messages_jan_2015.json > mongoimport_stocktwits_messages_jan_2015.out
