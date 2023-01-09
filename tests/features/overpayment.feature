@overpayment
Feature: Overpayment
  As DPS team
  I want metadata spreadsheet for overpayment tables
  So that the tables can be created in the system

  Background:
    Given Metadata spreadsheet "metadata.xlsx"

  Scenario Outline: Over payment ingestion tables
    Given Table <table>
    Then The metadata should be correctly created

    Examples:
    |table|
    |OP_AmendmentDetails| 

 