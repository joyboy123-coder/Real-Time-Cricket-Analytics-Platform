SELECT
    MATCH_ID,
    TEAM_1,
    TEAM_2,
    MATCH_NAME,
    MATCHTYPE,
    STATUS,

    CASE
        WHEN STATUS ILIKE '% won by %'
            THEN TRIM(
                REGEXP_REPLACE(
                    STATUS,
                    ' won by.*',
                    '',
                    1,
                    1,
                    'i'
                )
            )

        WHEN STATUS ILIKE '%won the super over%'
            THEN TRIM(
                REGEXP_REPLACE(
                    REGEXP_SUBSTR(
                        STATUS,
                        '[A-Za-z ]+ won the super over',
                        1,
                        1,
                        'i'
                    ),
                    ' won the super over',
                    '',
                    1,
                    1,
                    'i'
                )
            )

        ELSE NULL
    END AS WINNER,

    CASE
        WHEN STATUS ILIKE '%tied%' OR STATUS ILIKE '%tie%' OR STATUS ILIKE '%draw%'
            THEN 'Match Tied'

        WHEN STATUS ILIKE '%no result%' OR STATUS ILIKE '%no-result%'
            THEN 'No Result'

        WHEN STATUS ILIKE '%abandoned%'
            THEN 'Abandoned'

        WHEN STATUS ILIKE '%won by%' OR STATUS ILIKE '%won the super over%'
            THEN 'Completed'

        WHEN MATCHENDED = TRUE
            THEN 'Completed'

        WHEN MATCH_DATE < CURRENT_DATE
             AND (MATCHENDED = FALSE OR MATCHENDED IS NULL)
            THEN '⚠️ LIVE STATUS OUTDATED'

        ELSE 'Live'
    END AS RESULT_TYPE,

    CASE
        WHEN STATUS ILIKE '%won by%'
            THEN REGEXP_SUBSTR(
                STATUS,
                'won by.*',
                1,
                1,
                'i'
            )

        WHEN STATUS ILIKE '%won the super over%'
            THEN 'Won the Super Over'

        ELSE NULL
    END AS WIN_MARGIN,

    MATCH_DATE,
    MATCH_TIME,
    VENUE_NAME,
    CITY,
    MATCHSTARTED,
    MATCHENDED,
    FETCH_DATE

FROM {{ ref('stg_cricket_matches') }}