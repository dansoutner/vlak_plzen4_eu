#!/usr/bin/env bash

. vevn/bin/activate

python get_direct_connection_cli.py --from-stop "ST_44120" --to-stop "ST_44121" \
                                --from-label "Doubravka" --to-label "Hlavní nádraží" \
                                --html-out doubravka_hlavak.html

python get_direct_connection_cli.py --from-stop "ST_44121" --to-stop "ST_44120" \
                                --from-label "Hlavní nádraží" --to-label "Doubravka" \
                                --html-out hlavak_doubravka.html
