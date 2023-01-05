@daily
Feature: Process daily files
  As a PLICS team
  I want to produce

  @apcdaily
  Scenario: Successful process of APC daily file
    Given FeedType "APC", OrgSubmittingID "BRK01" and Month "M10" and OrgSubmissionID "00000001" is received
    Then the file is successfully processed and stored in Database 

@ecdaily
  Scenario: Successful process of EC daily file
    Given FeedType "EC", OrgSubmittingID "BRK01" and Month "M06" and OrgSubmissionID "00000002" is received
    Then the file is successfully processed and stored in Database 

@iaptdaily
  Scenario: Successful process of IAPT daily file
    Given FeedType "IAPT", OrgSubmittingID "BRK01" and Month "M12" and OrgSubmissionID "00000003" is received
    Then the file is successfully processed and stored in Database 

@csccdaily
  Scenario: Successful process of CSCC daily file
    Given FeedType "CSCC", OrgSubmittingID "BRK01" and Month "M06" and OrgSubmissionID "00000001" is received
    Then the file is successfully processed and stored in Database 