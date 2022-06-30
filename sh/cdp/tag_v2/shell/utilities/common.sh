#!/bin/bash

get_org_id(){ 
    output=`mysql --login-path=cdp -e "select id from cdp_organization.organization;" | tail +2 | sed -e "s/\t/\n/g"`
    echo $output
    }
