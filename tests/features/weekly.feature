@weekly
Feature: Process APC output CSV
  As a PLICS team
  I want to produce

  @apcweekly
  Scenario: Successful process of APC output file
    Given feed type APC, batch number 0 and OrgSubmissionID 00000001
    Then the output CSV file should be correctly produced

  @ecweekly
  Scenario: Successful process of EC output file
    Given feed type EC, batch number 0 and OrgSubmissionID 00000005
    Then the output CSV file should be correctly produced

  @opweekly
  Scenario: Successful process of OP output file
    Given feed type OP, batch number 0 and OrgSubmissionID 00000002
    Then the output CSV file should be correctly produced

  @siweekly
  Scenario: Successful process of SI output file
    Given feed type SI, batch number 0 and OrgSubmissionID 00000003
    Then the output CSV file should be correctly produced

  @swcweekly
  Scenario: Successful process of SWC output file
    Given feed type SWC, batch number 0 and OrgSubmissionID 00000004
    Then the output CSV file should be correctly produced

  @csccweekly
  Scenario: Successful process of CSCC output file
    Given feed type CSCC, batch number 0 and OrgSubmissionID 00000001
    Then the output CSV file should be correctly produced

  @mhccweekly
  Scenario: Successful process of MHCC output file
    Given feed type MHCC, batch number 3 and OrgSubmissionID 00000004
    Then the output CSV file should be correctly produced

  @mhpsweekly
  Scenario: Successful process of MHPS output file
    Given feed type MHPS, batch number 3 and OrgSubmissionID 00000005
    Then the output CSV file should be correctly produced

  @iaptweekly
  Scenario: Successful process of IAPT output file
    Given feed type IAPT, batch number 1 and OrgSubmissionID 00000009
    Then the output CSV file should be correctly produced

  @intrecweekly
  Scenario: Successful process of INTREC output file
    Given feed type INTREC, batch number 0 and OrgSubmissionID 00000001
    Then the REC output CSV file should be correctly produced
