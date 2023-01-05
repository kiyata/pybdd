from decimal import Decimal
import json
import os
from tabulate import tabulate
from decouple import config

# noinspection PyMethodMayBeStatic
class BaseUtil:

    def __init__(self):
        pass

    def beautifyCostData(self, FeedData, outputData):
        result = {
            'expected': f"{FeedData.ActCstID}|{FeedData.ActCnt}|{FeedData.ResCstID}|{FeedData.TotCst}",
            'actual': f"{outputData[0]}|{outputData[1]}|{outputData[2]}|{outputData[3]}"}
        return result

    def beautifyRecData(self, expected, outputData):
        result = {
            'expected': f"{expected[0]}|{expected[1]}|{expected[2]}|{expected[3]}|{expected[4]}",
            'actual': f"{outputData[0]}|{outputData[1]}|{outputData[2]}|{outputData[3]}|{outputData[4]}"}
        return result

    def castValue(self, value):
        nullables = [None, '', 'EMPTY', 'NILL']
        if isinstance(value, float):
            splitted = str(value).split('.')
            if splitted[1] == '0':
                return splitted[0]
            else:
                return value
        elif value in nullables:
            return 'NULL'
        else:
            return value

    def format_multi_line_result(self, fields, data):
        result = ""
        for key in fields:
            result += f"{self.cleanData(data[key])}|"
        return result.rstrip(result[-1])

    def cleanData(self, data):
        cleanedData = str(data).replace("'", "").replace("(", "").replace(")", "").replace(",", "")
        if data == 'NULL':
            return ""
        else:
            return cleanedData

    def formatFloat(self, data, decimals):
        try:
            return round(data, decimals)
        except:
            return data


    def trim_training_zeros(self, data):
        if data.find(".") != -1:
            return data.rstrip('0')
        else:
            return data

    # save data for report in cache
    def save_test_data_to_cache(self, request, Collection, EXPECTED, ACTUAL, ITERATION):
        request.config.cache.set('COLLECTION', Collection)
        request.config.cache.set('EXPECTED', EXPECTED)
        request.config.cache.set('ACTUAL', ACTUAL)
        request.config.cache.set('ITERATION', ITERATION)

    def get_query_data(self, context, feed_type):
        nonHesFeedTypes = ["CSCC", "MHCC", "MHPS"]
        noEpisodeFeedTypes = ["CSCC", "MHCC", "MHPS", "SI", "OP"]
        result = {
            "OrgSubmittingID": self.cleanData(context["OrgSubmittingID"]),
            "OrgProviderID": self.cleanData(context["OrgProviderID"])
        }
        if feed_type in nonHesFeedTypes:
            result["FeedType"] = self.cleanData(context["FeedType"])

        if feed_type == "CSCC":
            if self.cleanData(context["TeamType"]) != " ":
                result["TeamType"] = self.cleanData(context["TeamType"])
            if self.cleanData(context["ChsCcy"]) != "":
                result["ChsCcy"] = self.cleanData(context["ChsCcy"])
        if feed_type == "MHCC" or feed_type == "MHPS":
            if self.cleanData(context["PatCAS"]) != "":
                result["PatCAS"] = self.cleanData(context["PatCAS"])
            if self.cleanData(context["Cluster"]) != "":
                result["Cluster"] = self.cleanData(context["Cluster"])
        if feed_type not in nonHesFeedTypes:
            result["DeptCode"] = self.cleanData(context["DeptCode"])
            result["ServiceCode"] = self.cleanData(context["ServiceCode"])
            result["CurrencyCode"] = self.cleanData(context["CurrencyCode"])
        if feed_type not in noEpisodeFeedTypes:
            result["EpisodeType"] = self.cleanData(context["EpisodeType"])
        return result

    def get_report_fields(self, feed_type):

        fields = {
            "CSCC": ["OrgProviderID", "OrgSubmittingID", "FeedType", "TeamType", "ChsCcy", "TotalActivity",
                     "TotalCost", "Average_Cost", "National_Average_Cost", "Number_Trusts_Submitting"],
            "MHCC": ["OrgProviderID", "OrgSubmittingID", "FeedType", "PatCAS", "Cluster", "TotalActivity",
                     "TotalCost", "CC_AvgCostPatCASCluster", "CC_NatAvgCostActivity", "Number_Trusts_Submitting"],
            "APC": ["OrgSubmittingID", "OrgProviderID", "DeptCode", "ServiceCode", "CurrencyCode", "EpisodeType",
                    "TotalActivity", "TotalCost","Average_Cost", "National_Average_Cost", "Number_Trusts_Submitting"],
            "EC": ["OrgSubmittingID", "OrgProviderID", "DeptCode", "ServiceCode", "CurrencyCode", "EpisodeType",
                   "TotalActivity", "TotalCost","Average_Cost", "National_Average_Cost", "Number_Trusts_Submitting"],
            "OP": ["OrgSubmittingID", "OrgProviderID", "DeptCode", "ServiceCode", "CurrencyCode", "EpisodeType",
                   "TotalActivity", "TotalCost","Average_Cost", "National_Average_Cost", "Number_Trusts_Submitting"],
            "SI": ["OrgSubmittingID", "OrgProviderID", "DeptCode", "ServiceCode", "CurrencyCode", "EpisodeType",
                   "TotalActivity", "TotalCost","Average_Cost", "National_Average_Cost", "Number_Trusts_Submitting"],
            "SWC": ["OrgSubmittingID", "OrgProviderID", "DeptCode", "ServiceCode", "CurrencyCode", "EpisodeType",
                    "TotalActivity", "TotalCost","Average_Cost", "National_Average_Cost", "Number_Trusts_Submitting"]
        }

        return fields[feed_type]



