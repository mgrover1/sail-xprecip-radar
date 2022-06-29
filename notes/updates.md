# SAIL X-Band Precip Update

###### tags: `SAIL`

## 28 June 2022 3 PM CDT

### CMAC - Bobby
* Has the retrievals running using the dictionary in default_config.py
    * Can add/subtract retrievals from the cmac config by adding dictionaries with parameters
    * On full PPIs

### Beam Blockage
* Originally placing the radar in the mountains, leading to bad beam blockage map
* Waiting to get onto cumulus - sorting out a negative range issue

### Applying ZS Relationships
* Went through and applied ~10 different relationships
* All the notebooks are located [here](https://arm-development.github.io/sail-xprecip-radar/overview.html)

### Notes from Discussion
* Compare with KAZR - Z bias
* Should know about density of the snow
* PIPs? Image particle sensors? Camera snapshots as a function of time
* Look at which ZS relationships are most valid around Colorado
* Break down by events?
* 12 hourly accumulation
* Good to know which audience tailoring to - temporal, spatial resolution
* Uncertainties - maybe 2db? Apply that to the plot?
* Vary the A, B, and Z
* START WITH PLOTS COMPARING KAZR + X-Band



## 7 June 2022 11 AM CDT

### Post Processing + Corrections

### Gluing Files

### Running CMAC
* Got CMAC to run on March 2022 cases
    * Posted velocity plots in the slack
    * Continuing to check other cases

### Beam Blockage
* Partial beam blockage working
* Haven't heard back about the high resolution elevation data
    * Still on vacation?

### Precipitation Estimate
* https://arm-development.github.io/sail-xprecip-radar/radar-precip/csu-xband-snowfall-march-14-2022.html

### Next steps
* Look at precip retrievals from Christmas 2021 event
    * Using corrected data from Bobby
    * Using gothicwx station + ARM instruments as comparison


### General Status Update
* Report on state of X-Band snowfall retrievals: June 1st
    * Done - BAMS Figure
* First Matched datasets for winter 2022 datasets: June 1st
    * March 14 event done
    * Christmas 2021 event - just starting (June 7)
* QPE-Snow Ensemble product: August 1st
    * 3 Z(s) relationships
    * TODO
        * Add more xband relationships
        * Add variability to SWE values (other than just 8.5)
            * 5, 10, 15
* Verification notebooks: August 1st
    * Framework for this done - March 14 event
        * TODO - Christmas
        * Use Gothicwx page to identify events
        * Feb 22 case?
* First summer (rain) products: October 31st.
    * Not started (June 7)
* Continued product delivery: Through FY23.
    * Not started?