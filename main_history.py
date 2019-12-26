from postgres_notif_crawling import postgres_crawling

pilot = postgres_crawling()
pilot.load_data_to_bq_history()