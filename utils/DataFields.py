class DataFields:

    def get_activity_fields(self, feed_type):
        if feed_type == 'EC':
            return self.EC
        elif feed_type == 'APC':
            return self.APC
        elif feed_type == 'OP':
            return self.OP
        elif feed_type == 'SWC':
            return self.SWC
        elif feed_type == 'SI':
            return self.SI
        elif feed_type == 'MHCC':
            return self.MHCC
        elif feed_type == 'MHPS':
            return self.MHPS
        elif feed_type == 'IAPT':
            return self.IAPT
        elif feed_type == 'INTREC':
            return self.INTREC
        elif feed_type == 'MHREC':
            return self.MHREC
        else:
            raise AttributeError("Unsupported feed type")

    def CSCC(self):
        return [
            "OrgSubmissionID",
            "CollYear",
            "Mnth",
            "MessageBody_ORDINAL",
            "Activity_ORDINAL",
            "OrgSubmittingId",
            "OrgProviderID",
            "PLEMI",
            "NHSNo",
            "NhsSt",
            "DoB",
            "Gendr",
            "SerReqID",
            "CareId",
            "CareDte",
            "ClinDuration",
            "TeamType",
            "CCSubject",
            "ConsultType",
            "CMedium",
            "LocCode",
            "GPTherapyInd",
            "ChsCcy",
            "ORGActivityID",
            "Timestamp"
        ]

    def EC(self):
        return [
            "OrgSubmissionID",
            "CollYear",
            "Mnth",
            "MessageBody_ORDINAL",
            "Activity_ORDINAL",
            "OrgSubmittingID",
            "OrgProviderID",
            "CDSID",
            "NHSNo",
            "NhsSt",
            "Postcd",
            "DoB",
            "Gendr",
            "AEATTENDNO",
            "HRG",
            "DepDate",
            "DepTime",
            "DepTyp",
            "ArrDate",
            "ArrTime",
            "ORGActivityID",
            "Timestamp"
        ]

    def APC(self):
        return [
            "OrgSubmissionID",
            "PLEMI",
            "CollYear",
            "Mnth",
            "MessageBody_ORDINAL",
            "APCActivity_ORDINAL",
            "OrgSubmittingId",
            "OrgProviderID",
            "CDSID",
            "NHSNo",
            "NhsSt",
            "Postcd",
            "DoB",
            "Gendr",
            "Pod",
            "HSpellNo",
            "EpiNo",
            "EpStDte",
            "EpEnDte",
            "EpType",
            "HrgFce",
            "HrgSpl",
            "Alos",
            "PathId",
            "PatOrgId",
            "Tfc",
            "CFBand",
            "PartCost",
            "EpGro",
            "ORGActivityID",
            "Timestamp"
        ]

    def OP(self):
        return [
            "OrgSubmissionID",
            "CollYear",
            "Mnth",
            "MessageBody_ORDINAL",
            "Activity_ORDINAL",
            "OrgSubmittingId",
            "OrgProviderID",
            "CDSID",
            "PLEMI",
            "NHSNo",
            "NhsSt",
            "Postcd",
            "DoB",
            "Gendr",
            "Pod",
            "ATTENDID",
            "HRG",
            "AppDate",
            "AppTime",
            "Tfc",
            "CFBand",
            "WhActTyp",
            "PartCost",
            "PathId",
            "PatOrgId",
            "ORGActivityID",
            "Timestamp"
        ]

    def INTREC(self):
        return [
            "ReconciliationID",
            "CollYear",
            "OrgSubmittingID",
            "OrgProviderID",
            "ReconciliationLevel",
            "FinAccID",
            "ServID",
            "TotCst",
            "TotCstIncome",
            "Timestamp"
        ]

    def IAPT(self):
        return [
            "OrgSubmissionID",
            "MessageBody_ORDINAL",
            "Activity_ORDINAL",
            "OrgSubmittingID",
            "OrgProviderID",
            "NhsNo",
            "NhsSt",
            "PostCd",
            "Dob",
            "Gendr",
            "LptID",
            "SerReqID",
            "CareConID",
            "CareConDate",
            "CareConTime",
            "Attendance",
            "PatCAS",
            "Cluster",
            "Mnth",
            "CollYear",
            "Timestamp"
        ]

    def MHCC(self):
        return [
            "OrgSubmissionID",
            "MessageBody_ORDINAL",
            "Activity_ORDINAL",
            "OrgSubmittingID",
            "PLEMI",
            "OrgProviderID",
            "SerReqID",
            "CareId",
            "CareDte",
            "Attendance",
            "PatCAS",
            "Cluster",
            "Mnth",
            "CollYear",
            "Timestamp"
        ]

    def MHPS(self):
        return [
            "OrgSubmissionID",
            "MessageBody_ORDINAL",
            "Activity_ORDINAL",
            "OrgSubmittingID",
            "OrgProviderID",
            "PLEMI",
            "SerReqID",
            "HSpellId",
            "HSpllStDte",
            "HSpllDisDte",
            "SpllType",
            "PatCAS",
            "PatCASStDte",
            "PatCASEndDte",
            "Cluster",
            "StartDateCareClust",
            "EndDateCareClust",
            "Mnth",
            "CollYear",
            "Timestamp"
        ]

    def MHREC(self):
        return [
            "ReconciliationID",
            "CollYear",
            "OrgSubmittingID",
            "OrgProviderID",
            "ReconciliationLevel",
            "FinAccID",
            "ServID",
            "TotCst",
            "TotCstIncome",
            "Timestamp"
        ]

    def SI(self):
        return [
            "OrgSubmissionID",
            "MessageBody_ORDINAL",
            "Activity_ORDINAL",
            "OrgSubmittingID",
            "OrgProviderID",
            "PLEMI",
            "UnActDate",
            "CSIU",
            "UnCur",
            "Mnth",
            "CollYear",
            "Timestamp"
        ]

    def SWC(self):
        return [
            "OrgSubmissionID",
            "MessageBody_ORDINAL",
            "Activity_ORDINAL",
            "OrgSubmittingID",
            "OrgProviderID",
            "PLEMI",
            "UnAct",
            "CCLI",
            "CCUF",
            "UnActDate",
            "CCPerType",
            "OrgsSupp",
            "UnHRG",
            "Mnth",
            "CollYear",
            "Timestamp"
        ]
    def cost(self):
        return [
            "OrgSubmittingID",
            "FinYr",
            "OrgId",
            "Mnth",
            "Activity_ORDINAL",
            "ActCstID",
            "ActCnt",
            "ResCstID",
            "TotCst",
            "CstActivityID"
        ]