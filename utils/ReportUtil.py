from fpdf import FPDF, HTMLMixin
from decouple import config
from datetime import datetime


# creating a class inherited from both FPDF and HTMLMixin
class ReportFPDF(FPDF, HTMLMixin):
    pass


# noinspection PyMethodMayBeStatic
class ReportUtil:

    def __init__(self):
        pass

    def printReport(self, data, data_type, job_type, feed_type):
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        file_suffix = now.strftime("%Y%m%d%H%M%S")
        REPORT_DIR = f"{config('REPORT_DIR')}"
        template = REPORT_DIR + "/report_template.htm"
        output_file = REPORT_DIR + f"/{feed_type}_{data_type}_results.html"
        if feed_type in ["INTREC", "AMBREC","IAPTREC"]:
            output_file = REPORT_DIR + f"/{feed_type}_results.html"

        totals = self.getTotals(data)
        with open(template, 'r') as f:
            html = f.read()\
                .replace("CONTENTS", self.tableItem(data))\
                .replace("JOBTYPE", job_type.capitalize())\
                .replace("TIMESTAMP", current_time)\
                .replace("DATATYPE", data_type.capitalize())\
                .replace("PASSEDTESTS", totals['passed'])\
                .replace("FAILEDTESTS", totals['failed'])

        with open(output_file, 'w') as f:
            f.write(html)

    def printMatchingReport(self, data, feed_type, totalPassed, totalFailed):
        REPORT_DIR = f"{config('REPORT_DIR')}"
        template = REPORT_DIR + "/report_template.htm"
        output_file = REPORT_DIR + f"/{feed_type}_Matching_Results.html"

        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")

        with open(template, 'r') as f:
            html = f.read() \
                .replace("CONTENTS", self.mathingTableItem(data)) \
                .replace("JOBTYPE", "Matching") \
                .replace("TIMESTAMP", current_time) \
                .replace("DATATYPE", "") \
                .replace("PASSEDTESTS", str(totalPassed)) \
                .replace("FAILEDTESTS", str(totalFailed))

        with open(output_file, 'w') as f:
            f.write(html)


    def tableItem(self, data):
        contents = ""
        header = """
        <div class="table-content">
            <table class="table-view">
                <tbody>
                    <tr class="table-header">
                        <th>Field </th>
                        <th>Expected </th>
                        <th>Actual </th>
                        <th>Result</th>
                    </tr>
        """
        footer = """
                </tbody>
            </table>
        </div>
        """
        for item in data:
            contents += header

            for entry in item:
                entry_content = '<tr>'
                entry_content += f"<td>{entry['field']}</td>"
                entry_content += f"<td>{entry['expected']}</td>"
                entry_content += f"<td>{entry['actual']}</td>"
                entry_content += f"<td class=\"{entry['result'].lower()}\">{entry['result']}</td>"
                entry_content += "</tr>"
                contents += entry_content
            contents += footer
        return contents

    def getTotals(self, data):
        PASSED = 0
        FAILED = 0
        for item in data:
            for entry in item:
                if entry['result'] == 'OK':
                    PASSED += 1
                else:
                    FAILED += 1
        return {'passed': str(PASSED), 'failed': str(FAILED)}

    def mathingTableItem(self, data):
        contents = ""
        header = """
            <div class="table-content">
                <table class="table-view">
                    <tbody>
            """
        footer = """
                    </tbody>
                </table>
            </div>
            """

        for entry in data:
            contents += header
            entry_content = '<tr>'
            entry_content += "<td>ActivityRowID</td>"
            entry_content += f"<td>{entry['ActivityRowID']}</td>"
            entry_content += '</tr>'
            entry_content = '<tr>'
            entry_content += "<td>Criteria</td>"
            entry_content += f"<td>{entry['matchCriteria']}</td>"
            entry_content += '</tr>'
            entry_content += '<tr>'
            entry_content += "<td>Values matched</td>"
            entry_content += f"<td>{entry['matchedValues']}</td>"
            entry_content += '</tr>'
            entry_content += '<tr>'
            entry_content += "<td>Matched Identifier</td>"
            entry_content += f"<td>{entry['identifier']}</td>"
            entry_content += '</tr>'
            entry_content += '<tr>'
            entry_content += "<td>Matching code</td>"
            entry_content += f"<td>{entry['matchCode']}</td>"
            entry_content += '</tr>'
            entry_content += '<tr>'
            entry_content += "<td>Result</td>"
            entry_content += f"<td class=\"{entry['result'].lower()}\">{entry['result']}</td>"
            entry_content += "</tr>"
            contents += entry_content
            contents += footer
        return contents

    def getMatchingTotals(self, data):
        PASSED = 0
        FAILED = 0
        for entry in data:
            print(entry)
            if entry['result'] == 'OK':
                PASSED += 1
            else:
                FAILED += 1
        return {'passed': str(PASSED), 'failed': str(FAILED)}