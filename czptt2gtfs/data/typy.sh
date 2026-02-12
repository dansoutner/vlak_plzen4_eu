#!/bin/bash


curl -v http://provoz.szdc.cz/kadrws/ciselniky.asmx -H "Content-Type: application/soap+xml; charset=utf-8"  -d '<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <SeznamTypuSpolecnosti xmlns="http://provoz.szdc.cz/kadr">
      <jenAktulnePlatne>true</jenAktulnePlatne>
    </SeznamTypuSpolecnosti>
  </soap12:Body>
</soap12:Envelope>'

