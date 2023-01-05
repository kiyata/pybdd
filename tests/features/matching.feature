@matching
Feature: Match PLICS data
  As a PLICS team
  I want to match costs data against HES data

  @apcmatching
  Scenario: Verify APC matching
    Given feed type APC, batch number 000, xml file APC_FY2021-22_M01_RRK_20220809T0913_01.xml, matching data apc_matching.csv and OrgSubmissionID 000000001
    Then the matching value in CSV should be correct

  @ecmatching
  Scenario: Verify EC matching
    Given feed type EC, batch number 000, xml file EC_FY2021-22_M04_RRK_20220809T0913_01.xml, matching data ec_matching.csv and OrgSubmissionID 000000001
    Then the matching value in CSV should be correct


  @opmatching
  Scenario: Verify OP matching
    Given feed type OP, batch number 000, xml file OP_FY2021-22_M10_RRK_20220809T0913_01.xml, matching data op_matching.csv and OrgSubmissionID 000000001
    Then the matching value in CSV should be correct

  @mhccmatching
  Scenario: Verify MHCC matching
    Given feed type MHCC, batch number 000, xml file MHCC_FY2021-22_M11_RRK_20220901T0858_01.xml, matching data mhcc_matching.csv and OrgSubmissionID 000000001
    Then the matching value in CSV should be correct

  @mhpsmatching
  Scenario: Verify MHPS matching
    Given feed type MHPS, batch number 000, xml file MHPS_FY2021-22_M02_RRK_20220901T0859_01.xml, matching data mhps_matching.csv and OrgSubmissionID 000000002
    Then the matching value in CSV should be correct

  @iaptmatching
  Scenario: Verify IAPT matching
    Given feed type IAPT, batch number 000, xml file IAPT_FY2021-22_M01_OR1_20220920T1706_01.xml, matching data iapt_matching.csv and OrgSubmissionID 000000008
    Then the matching value in CSV should be correct

