import sys
import shutil
import os

import make_sample_sets as mod
from make_sample_sets import *

#Please note this script needs to be copied into the validation scripts import folder for it to work.

#run validation scripts
def validate(db_name, transitionSize = None):
    
    mod.workingDb['path'] = db_name
	
    mod.runStep(lambda: updateTableUnits('subject'), msg = 'Updating to common units in subject table')
	
    mod.runStep(lambda: updateTableUnits('timeSeries'), msg = 'Updating to common units in timeSeries table')
	
    mod.runStep(addComputedFields, msg = 'Adding computed fields (e.g. MAP, NLR)')
    mod.runStep(addComputedFields, msg = 'Adding computed fields (e.g. MAP, NLR)')
	
    baselineFilter = "day >= -10 and day <=2"
    mod.runStep(lambda: makeBaseline(baselineFilter = baselineFilter), msg = f'Making the baseline table using "{baselineFilter}"')
	
    mod.runStep(lambda: makeDataset(paths.out('baseline.csv')), msg = 'Making the baseline dataset')
	
    # Version of baseline that only has DEATH or DISCHARGED
    baselineFilter = "day >= -10 and day <=2 and usubjid in (select usubjid from subject where dsdecod regexp 'DEATH|DISCHARGED')" # and ci_CRP is not null"
    mod.runStep(lambda: makeBaseline(baselineFilter = baselineFilter, tableName = 'baseline_resolvedOnly'), msg = f'Making the baseline table using "{baselineFilter}"')
    mod.runStep(lambda: makeDataset(paths.out('baseline_resolvedOnly.csv'), query='select * from subject inner join baseline_resolvedOnly on subject.usubjid = baseline_resolvedOnly.usubjid'), msg = 'Making the baseline (resolved cases only) dataset')
	
    # Uncomment to pick the worst status within the 3 day window. Otherwise, chooses the status nearest the midpoint
    mod.runStep(lambda: makeDbn2Slice(choose = 'nearest',
        # specialCols = {'ci_ds_decod': 'iif(sum(ci_ds_decod="DEATH") over SUBJECTTIMEWINDOW, "DEATH", iif(sum(ci_ds_decod="DISCHARGED") over SUBJECTTIMEWINDOW, "DISCHARGED", min(ci_ds_decod) filter (where ci_ds_decod is not null) over SUBJECTTIMEWINDOW))'},
        transitionSize=transitionSize), msg = 'Making dbn2Slice table')
	
    mod.runStep(cleanDbn2Slice, msg = 'Padding out the DS_DECOD field for slice t1')
    mod.runStep(updateDbn2Slice, msg = 'Updating dbn2slice with +1 day variables')

    # Note: this is skipped by default, since progression accesses database directly
    mod.runStep(lambda: makeDataset(paths.out('dbn2slice.csv'), query = 'select * from subject left join dbn2slice on subject.usubjid = dbn2slice.usubjid_t0'), msg = 'Making DBN 2 slice data set (discretized)', skipWhen = True)

if __name__ == '__main__':
    validate('../data_output/covidi.db')