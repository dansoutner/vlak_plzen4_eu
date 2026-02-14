#!/usr/bin/env bash

. venv/bin/activate

#python get_direct_connection_cli.py --from-stop "ST_44120" --to-stop "ST_44121" \
#                                --from-label "Doubravka" --to-label "Hlavní nádraží" \
#                                --html-out doubravka_hlavak.html \
#                                --gtfs-path data/official_rail_work/official_gtfs/2026


#python get_direct_connection_cli.py --from-stop "ST_44121" --to-stop "ST_44120" \
#                                --from-label "Hlavní nádraží" --to-label "Doubravka" \
#                                --html-out hlavak_doubravka.html \
#                                --gtfs-path data/official_rail_work/official_gtfs/2026

python get_direct_connection_cli.py --from-stop "73265" --to-stop "73275" \
                                --from-label "Doubravka" --to-label "Hlavní nádraží" \
                                --html-out doubravka_hlavak.html \
                                --gtfs-path data/official_rail_work/official_gtfs/2026


python get_direct_connection_cli.py --from-stop "73275" --to-stop "73265" \
                                --from-label "Hlavní nádraží" --to-label "Doubravka" \
                                --html-out hlavak_doubravka.html \
                                --gtfs-path data/official_rail_work/official_gtfs/2026
