
"""
Identify the second lowest cost silver plan (slcsp) for the provided zipcodes 
whenever an unambiguous value exists.  Return results as a csv file and print
to stdout.
"""
import pandas as pd


def main():
    ## Load relevant columns from csv files into dataframes (df).  Load zipcodes
    # as strings so that leading zeroes are not lost by being autoconverted to 
    # integers.
    plans = pd.read_csv(
        'plans.csv', 
        usecols=['metal_level', 'rate', 'rate_area']
        )
    zips_and_rate_areas = pd.read_csv(
        'zips.csv', 
        usecols=['zipcode', 'rate_area'], 
        dtype={'zipcode': str}
        )        
    target_zips = pd.read_csv(
        'slcsp.csv',
        usecols=['zipcode'],
        dtype={'zipcode': str}
        )

    # Restrict attention to silver plans and the zipcodes for which slcsps 
    # have been requested
    silver_plans = plans[plans['metal_level'] == 'Silver']
    zips_and_rate_areas = zips_and_rate_areas[zips_and_rate_areas['zipcode'].isin(
        target_zips.values.flatten()
        )
    ]

    # Only consider rate areas in which more than one silver plan is offered,
    # since we need at least two silver plans for an slcsp to exist
    plan_area_counts = silver_plans['rate_area'].value_counts()
    has_multiple_plans = plan_area_counts.values > 1
    multi_plan_areas = plan_area_counts[has_multiple_plans].index
    multi_plan_rates = silver_plans[silver_plans['rate_area'].isin(
        multi_plan_areas
        )
    ]

    # Many-to-many relation between zipcodes and rate_areas => inner and outer 
    # joins give same results.  Default is 'inner'.
    zips_and_rates = pd.merge(
        zips_and_rate_areas, 
        multi_plan_rates,  
        on='rate_area'
        )

    # Create df where each row has a unique (zipcode, rate_area) combination
    unique_zip_rate_areas = zips_and_rates.drop_duplicates(
        subset=['zipcode', 'rate_area']
        )

    # Frequency counts then give # of rate areas in a particular zipcode
    num_rate_areas_per_zip = unique_zip_rate_areas['zipcode'].value_counts()

    # Identify zipcodes that map to a single rate area, and extract the
    # associated plan data
    is_unambiguous = num_rate_areas_per_zip.values == 1
    unambiguous_zips = num_rate_areas_per_zip[is_unambiguous].index
    unambiguous_plans = zips_and_rates[zips_and_rates['zipcode'].isin(
        unambiguous_zips
        )
    ]

    # set & sorted => unique and ascending rates, so we can safely take 
    # second element as slcsp without having to separately check for special 
    # case of repeated lowest rates
    slcsp_by_zip = unambiguous_plans.groupby('zipcode').agg(
        {'rate':lambda x: sorted(set(x))[1]}
        ).reset_index() # convert multi-index df from groupby to standard index

    results = pd.merge(
        target_zips, 
        slcsp_by_zip,
        how='left', # left join restricts to tartget_zips and preserves ordering
        on='zipcode'
        ).fillna('').set_index('zipcode')

    # Save results to file and print to stdout
    results.to_csv('slcsp_results.csv')
    print(results.to_csv(None))


if __name__ == '__main__':
    main()