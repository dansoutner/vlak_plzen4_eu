#!/bin/bash

# Endpoint URL
url="https://provoz.spravazeleznic.cz/kadrws/ciselniky.asmx"

# Set parameter values (update these as required)
jenAktualnePlatne_value="true"  # Use "true" or "false"
typSpolecnosti_value="1"        # Example short/integer value

# Define the SOAP envelope with parameters inserted
soap_envelope='<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
    xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <SeznamSpolecnostiUrcitehoTypu xmlns="http://provoz.szdc.cz/kadr">
      <jenAktualnePlatne>'"$jenAktualnePlatne_value"'</jenAktualnePlatne>
      <typSpolecnosti>'"$typSpolecnosti_value"'</typSpolecnosti>
    </SeznamSpolecnostiUrcitehoTypu>
  </soap12:Body>
</soap12:Envelope>'

# Send the SOAP request with curl
curl -X POST \
     -H "Content-Type: application/soap+xml; charset=utf-8" \
     --data "$soap_envelope" \
     "$url"
