# My solution to a coding challenge to calculate the second lowest cost silver plan healthcare benchmark (problem statement shown below)
I first load the data (CSV files), then parse and populate appropriately constructed Pandas dataframes with the cleaned up data.  Attention
is then restricted to 'rate areas' in which more than one silver plan is offered - using filters and a many-to-many join - since 
a given rate area needs to offer at least two silver plans for an slcsp to exist.

I then create a new dataframe where each row has a unique (zipcode, rate_area) combination so that frequency counts can be used to find the
number of rate areas in a particular zipcode, identify zipcodes that map to a single rate area, and extract the associated plan data.  Finally,
set & sorted are used to extract unique rates in ascending order, so we can safely take the second element as the slcsp without having to 
separately check for the special case of repeated lowest rates.

# Instructions:  Calculate second lowest cost silver plan (SLCSP)

## Problem

You have been asked to determine the second lowest cost silver plan (SLCSP) for
a group of ZIP Codes.

## Task

You have been given a CSV file, `slcsp.csv`, which contains the ZIP Codes in the
first column. Fill in the second column with the rate (see below) of the
corresponding SLCSP. Your answer is the modified CSV file, which the program
should __emit on stdout__, plus any source code used.

Write your code in your best programming language.

### Expected output

The order of the rows in your answer as emitted on stdout must stay the same as how they
appeared in the original `slcsp.csv`. The first row should be the column headers: `zipcode,rate`
The remaining lines should output unquoted values with two digits after the decimal
place of the rates, for example: `64148,245.20`.

It may not be possible to determine a SLCSP for every ZIP Code given. Check for cases
where a definitive answer cannot be found and leave those cells blank in the output CSV (no
quotes or zeroes or other text). For example, `40813,`

## Additional information

The SLCSP is the so-called "benchmark" health plan in a particular area. It is
used to compute the tax credit that qualifying individuals and families receive
on the marketplace. It is the second lowest rate for a silver plan in the rate area.

For example, if a rate area had silver plans with rates of `[197.3, 197.3,
201.1, 305.4, 306.7, 411.24]`, the SLCSP for that rate area would be `201.1`,
since it is the second lowest rate in that rate area.

A plan has a "metal level", which can be either Bronze, Silver, Gold, Platinum,
or Catastrophic. The metal level is indicative of the level of coverage the plan
provides.

A plan has a "rate", which is the amount that a consumer pays as a monthly
premium, in dollars.

A plan has a "rate area", which is a geographic region in a state that
determines the plan's rate. A rate area is a tuple of a state and a number, for
example, NY 1, IL 14.

There are two additional CSV files in this directory besides `slcsp.csv`:

  * `plans.csv` -- all the health plans in the U.S. on the marketplace
  * `zips.csv` -- a mapping of ZIP Code to county/counties & rate area(s)

A ZIP Code can potentially be in more than one county. If the county can not be
determined definitively by the ZIP Code, it may still be possible to determine
the rate area for that ZIP Code.

A ZIP Code can also be in more than one rate area. In that case, the answer is ambiguous
and should be left blank.

We will want to compile your code from source and run it, so please include the
complete instructions for doing so in a COMMENTS file.
