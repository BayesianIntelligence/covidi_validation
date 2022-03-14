# COVID Intelligence IDDO

Code and scripts (no data!) for performing the IDDO analysis.

Make sure:

1. Your data folder lives **outside** the repository
2. Your processing folder lives **outside** the repository
3. Your respository folder is **encrypted**
4. Avoid checking in anything other than scripts, documentation and initialisation files (initial BNs, configs, etc.)!

Main scripts:

<table>
    <tr><th>Script<th>Description
    <tr>
        <td>make_sample_sets.py
        <td>Main script for processing the IDDO and generating output databases and files.
    <tr>
        <td>make_nb.py
        <td>Creates NB/TAN/etc. predictive models based on everything in the baseline table
        (i.e. static info + day 1 info)
</table>

# Useful SQL queries:

Group by intervention category, and list all distinct treatments in that category:

```select incat, group_concat(distinct intrt) from `in` group by incat```

# Processing rules

## Interventions

Algo for filling in treatment days:
* If instdy and inendy specified, use and add all in between days
    * Check indur for consistency, report if not
* else if indy and indur specified, treat indy as end day and indur as duration (but group all oxygen therapies together)
* else if indur is blank and indy present, for incat/intrt/inmodify, then collect sequence of indy for subject, and "interpolate" binary, assuming 1 for no start day
    * if row['inoccur'] == 'Y':
        * Spread Ys from the current day back to the last change
    * elif row['inoccur'] == 'N' and lastSeenOccur == 'Y':
        * Spread Ys from the day of last change forward to the day before the current day
    * else (if row['inoccur'] = 'N' and lastSeenOccur = 'N'):
        * i.e. Don't record anything for this treatment from the last change to the current day
