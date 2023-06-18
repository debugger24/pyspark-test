SELECT (CASE
            WHEN ((A.cpg1__cpg2 IS NULL
                   AND B.cpg1__cpg2 IS NULL)
                  OR ((A.cpg1__cpg2=B.cpg1__cpg2))) THEN 1
            WHEN ((FALSE)) THEN 2
            ELSE 0
        END) AS cpg1__cpg2,
       A.cpg1__cpg2 AS cpg1__cpg2_base,
       B.cpg1__cpg2 AS cpg1__cpg2_compare,
       (CASE
            WHEN ((A.cpg1__cpg3__cpg4[0]__cpg5 IS NULL
                   AND B.cpg1__cpg3__cpg4[0]__cpg5 IS NULL)
                  OR ((A.cpg1__cpg3__cpg4[0]__cpg5=B.cpg1__cpg3__cpg4[0]__cpg5)
                      OR ((abs(A.cpg1__cpg3__cpg4[0]__cpg5-B.cpg1__cpg3__cpg4[0]__cpg5))<=(0+(0*abs(A.cpg1__cpg3__cpg4[0]__cpg5)))))) THEN 1
            WHEN ((FALSE)) THEN 2
            ELSE 0
        END) AS cpg1__cpg3__cpg4[0]__cpg5,
        A.cpg1__cpg3__cpg4[0]__cpg5 AS cpg1__cpg3__cpg4[0]__cpg5_base,
        B.cpg1__cpg3__cpg4[0]__cpg5 AS cpg1__cpg3__cpg4[0]__cpg5_compare,
                                                        A.id,
                                                        (CASE
                                                            WHEN ((A.list[0] IS NULL
                                                                    AND B.list[0] IS NULL)
                                                                    OR ((A.list[0]=B.list[0]))) THEN 1
                                                            WHEN ((FALSE)) THEN 2
                                                            ELSE 0
                                                        END) AS list[0],
                                                        A.list[0] AS list[0]_base,
                                                        B.list[0] AS list[0]_compare,
                                                        (CASE
                                                            WHEN ((A.list[1] IS NULL
                                                                    AND B.list[1] IS NULL)
                                                                    OR ((A.list[1]=B.list[1]))) THEN 1
                                                            WHEN ((FALSE)) THEN 2
                                                            ELSE 0
                                                        END) AS list[1],
                                                        A.list[1] AS list[1]_base,
                                                        B.list[1] AS list[1]_compare