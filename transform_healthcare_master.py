"""
Transform healthcare_powerbi_master.pbix (PBIR Enhanced Report Format)
Compliant with Healthcare Analytics Dashboard Specifications:
- Preserves full data model and data source
- Removes pink page background -> Sets clean white/light-blue background (#F4F7FB)
- Dark blue hospital-themed header: "Hospital Readmission Analytics Dashboard"
- 4 Aligned KPI Cards in one row:
  1. Total Admissions
  2. Average Length of Stay (Days)
  3. Average Predicted Readmission Risk
  4. Patient Count (distinct count of patient_hash)
- Horizontal Bar Chart: Average Length of Stay by Diagnosis Code (data labels ON, clean styling)
- Donut Chart: Patient Risk Distribution (HIGH_RISK, ELEVATED_RISK, LOW_RISK)
- Clustered Column Chart: Readmission Risk by Prior Inpatient Admissions (data labels ON, clean styling)
"""

import os
import sys
import shutil
import zipfile
import json

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_PBIX = os.path.join(ROOT_DIR, "healthcare_powerbi_master.pbix")
BACKUP_PBIX = os.path.join(ROOT_DIR, "healthcare_powerbi_master_backup.pbix")
FINAL_PBIX = os.path.join(ROOT_DIR, "Hospital_Readmission_Dashboard.pbix")

def transform_pbir():
    print("==========================================================================")
    print("🏥 TRANSFORMING HEALTHCARE_POWERBI_MASTER.PBIX (PBIR FORMAT)")
    print("==========================================================================")

    if not os.path.exists(INPUT_PBIX):
        raise FileNotFoundError(f"Missing {INPUT_PBIX}")

    # 1. Save Backup
    shutil.copyfile(INPUT_PBIX, BACKUP_PBIX)
    print(f"1. Saved backup to: {BACKUP_PBIX}")

    with zipfile.ZipFile(INPUT_PBIX, "r") as z_in:
        file_entries = {name: z_in.read(name) for name in z_in.namelist()}

    # 2. Modify page.json (Remove Pink Background -> Set Clean #F4F7FB)
    page_key = "Report/definition/pages/602ddcd746b2159a7a83/page.json"
    if page_key in file_entries:
        page_data = json.loads(file_entries[page_key].decode("utf-8"))
        page_data["objects"] = {
            "background": [{
                "properties": {
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#F4F7FB'"}}}}},
                    "transparency": {"expr": {"Literal": {"Value": "0D"}}}
                }
            }]
        }
        file_entries[page_key] = json.dumps(page_data, indent=2).encode("utf-8")
        print("2. Removed pink background; set clean #F4F7FB canvas background.")

    # 3. Create Dark Blue Header Visual
    header_visual = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.13.0/schema.json",
        "name": "header_title_bar",
        "position": {
            "x": 30,
            "y": 20,
            "z": 0,
            "height": 95,
            "width": 1860,
            "tabOrder": 0
        },
        "visual": {
            "visualType": "textbox",
            "objects": {
                "general": [{
                    "properties": {
                        "paragraphs": [
                            {
                                "textRuns": [{
                                    "value": "Hospital Readmission Analytics Dashboard\n",
                                    "textStyle": {
                                        "fontFamily": "'Segoe UI Semibold', wf_segoe-ui_semibold, sans-serif",
                                        "fontSize": "22pt",
                                        "fontWeight": "bold",
                                        "color": "#FFFFFF"
                                    }
                                }]
                            },
                            {
                                "textRuns": [{
                                    "value": "Patient Outcomes  |  Length of Stay  |  Readmission Risk",
                                    "textStyle": {
                                        "fontFamily": "'Segoe UI', wf_segoe-ui_normal, sans-serif",
                                        "fontSize": "11pt",
                                        "color": "#BAE6FD"
                                    }
                                }]
                            }
                        ]
                    }
                }]
            },
            "visualContainerObjects": {
                "background": [{
                    "properties": {
                        "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#0A2540'"}}}}},
                        "transparency": {"expr": {"Literal": {"Value": "0D"}}}
                    }
                }],
                "border": [{
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#1E3A8A'"}}}}},
                        "radius": {"expr": {"Literal": {"Value": "8D"}}}
                    }
                }]
            }
        }
    }
    file_entries["Report/definition/pages/602ddcd746b2159a7a83/visuals/header_title_bar/visual.json"] = json.dumps(header_visual, indent=2).encode("utf-8")
    print("3. Created dark-blue hospital header with Title and Subtitle.")

    # 4. Configure Four Aligned KPI Cards in One Row (Y: 130, Height: 125, Width: 444, Gap: 28)
    # Card 1: Total Admissions (81f120df1b8c73c27791 reused for Total Admission)
    card1_key = "Report/definition/pages/602ddcd746b2159a7a83/visuals/81f120df1b8c73c27791/visual.json"
    card1_data = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.13.0/schema.json",
        "name": "81f120df1b8c73c27791",
        "position": {"x": 30, "y": 130, "z": 1, "height": 125, "width": 444, "tabOrder": 1},
        "visual": {
            "visualType": "cardVisual",
            "query": {
                "queryState": {
                    "Data": {
                        "projections": [{
                            "field": {"Measure": {"Expression": {"SourceRef": {"Entity": "healthcare_powerbi_master"}}, "Property": "Total Admission"}},
                            "queryRef": "healthcare_powerbi_master.Total Admission",
                            "nativeQueryRef": "Total Admission"
                        }]
                    }
                }
            },
            "visualContainerObjects": {
                "title": [{
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "text": {"expr": {"Literal": {"Value": "'Total Admissions'"}}},
                        "fontColor": {"solid": {"color": {"expr": {"Literal": {"Value": "'#475569'"}}}}},
                        "fontSize": {"expr": {"Literal": {"Value": "11D"}}},
                        "bold": {"expr": {"Literal": {"Value": "true"}}}
                    }
                }],
                "background": [{
                    "properties": {
                        "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#FFFFFF'"}}}}},
                        "transparency": {"expr": {"Literal": {"Value": "0D"}}}
                    }
                }],
                "border": [{
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#E2E8F0'"}}}}},
                        "radius": {"expr": {"Literal": {"Value": "8D"}}}
                    }
                }]
            },
            "drillFilterOtherVisuals": True
        }
    }
    file_entries[card1_key] = json.dumps(card1_data, indent=2).encode("utf-8")

    # Card 2: Average Length of Stay (Days) (New visual container)
    card2_data = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.13.0/schema.json",
        "name": "kpi_avg_length_of_stay",
        "position": {"x": 502, "y": 130, "z": 2, "height": 125, "width": 444, "tabOrder": 2},
        "visual": {
            "visualType": "cardVisual",
            "query": {
                "queryState": {
                    "Data": {
                        "projections": [{
                            "field": {"Measure": {"Expression": {"SourceRef": {"Entity": "healthcare_powerbi_master"}}, "Property": "Average Stay"}},
                            "queryRef": "healthcare_powerbi_master.Average Stay",
                            "nativeQueryRef": "Average Stay"
                        }]
                    }
                }
            },
            "visualContainerObjects": {
                "title": [{
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "text": {"expr": {"Literal": {"Value": "'Average Length of Stay (Days)'"}}},
                        "fontColor": {"solid": {"color": {"expr": {"Literal": {"Value": "'#475569'"}}}}},
                        "fontSize": {"expr": {"Literal": {"Value": "11D"}}},
                        "bold": {"expr": {"Literal": {"Value": "true"}}}
                    }
                }],
                "background": [{
                    "properties": {
                        "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#FFFFFF'"}}}}},
                        "transparency": {"expr": {"Literal": {"Value": "0D"}}}
                    }
                }],
                "border": [{
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#E2E8F0'"}}}}},
                        "radius": {"expr": {"Literal": {"Value": "8D"}}}
                    }
                }]
            },
            "drillFilterOtherVisuals": True
        }
    }
    file_entries["Report/definition/pages/602ddcd746b2159a7a83/visuals/kpi_avg_length_of_stay/visual.json"] = json.dumps(card2_data, indent=2).encode("utf-8")

    # Card 3: Average Predicted Readmission Risk (248952141d37001f6d55)
    card3_key = "Report/definition/pages/602ddcd746b2159a7a83/visuals/248952141d37001f6d55/visual.json"
    card3_data = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.13.0/schema.json",
        "name": "248952141d37001f6d55",
        "position": {"x": 974, "y": 130, "z": 3, "height": 125, "width": 444, "tabOrder": 3},
        "visual": {
            "visualType": "cardVisual",
            "query": {
                "queryState": {
                    "Data": {
                        "projections": [{
                            "field": {
                                "Aggregation": {
                                    "Expression": {"Column": {"Expression": {"SourceRef": {"Entity": "healthcare_powerbi_master"}}, "Property": "readmission_risk_score"}},
                                    "Function": 1
                                }
                            },
                            "queryRef": "Average(healthcare_powerbi_master.readmission_risk_score)",
                            "nativeQueryRef": "Average of readmission_risk_score"
                        }]
                    }
                }
            },
            "visualContainerObjects": {
                "title": [{
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "text": {"expr": {"Literal": {"Value": "'Average Predicted Readmission Risk'"}}},
                        "fontColor": {"solid": {"color": {"expr": {"Literal": {"Value": "'#475569'"}}}}},
                        "fontSize": {"expr": {"Literal": {"Value": "11D"}}},
                        "bold": {"expr": {"Literal": {"Value": "true"}}}
                    }
                }],
                "background": [{
                    "properties": {
                        "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#FFFFFF'"}}}}},
                        "transparency": {"expr": {"Literal": {"Value": "0D"}}}
                    }
                }],
                "border": [{
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#E2E8F0'"}}}}},
                        "radius": {"expr": {"Literal": {"Value": "8D"}}}
                    }
                }]
            },
            "drillFilterOtherVisuals": True
        }
    }
    file_entries[card3_key] = json.dumps(card3_data, indent=2).encode("utf-8")

    # Card 4: Patient Count (Distinct count of patient_hash)
    card4_data = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.13.0/schema.json",
        "name": "kpi_patient_count",
        "position": {"x": 1446, "y": 130, "z": 4, "height": 125, "width": 444, "tabOrder": 4},
        "visual": {
            "visualType": "cardVisual",
            "query": {
                "queryState": {
                    "Data": {
                        "projections": [{
                            "field": {
                                "Aggregation": {
                                    "Expression": {"Column": {"Expression": {"SourceRef": {"Entity": "healthcare_powerbi_master"}}, "Property": "patient_hash"}},
                                    "Function": 6
                                }
                            },
                            "queryRef": "DistinctCountNonNull(healthcare_powerbi_master.patient_hash)",
                            "nativeQueryRef": "Patient Count"
                        }]
                    }
                }
            },
            "visualContainerObjects": {
                "title": [{
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "text": {"expr": {"Literal": {"Value": "'Patient Count'"}}},
                        "fontColor": {"solid": {"color": {"expr": {"Literal": {"Value": "'#475569'"}}}}},
                        "fontSize": {"expr": {"Literal": {"Value": "11D"}}},
                        "bold": {"expr": {"Literal": {"Value": "true"}}}
                    }
                }],
                "background": [{
                    "properties": {
                        "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#FFFFFF'"}}}}},
                        "transparency": {"expr": {"Literal": {"Value": "0D"}}}
                    }
                }],
                "border": [{
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#E2E8F0'"}}}}},
                        "radius": {"expr": {"Literal": {"Value": "8D"}}}
                    }
                }]
            },
            "drillFilterOtherVisuals": True
        }
    }
    file_entries["Report/definition/pages/602ddcd746b2159a7a83/visuals/kpi_patient_count/visual.json"] = json.dumps(card4_data, indent=2).encode("utf-8")
    print("4. Configured 4 aligned KPI cards: Total Admissions, Avg Stay, Predicted Risk, Patient Count.")

    # 5. Horizontal Bar Chart (6638f3fc91693bb94d82): Average of length_of_stay by diagnosis_code
    bar_key = "Report/definition/pages/602ddcd746b2159a7a83/visuals/6638f3fc91693bb94d82/visual.json"
    bar_data = json.loads(file_entries[bar_key].decode("utf-8"))
    bar_data["position"] = {"x": 30, "y": 275, "z": 5, "height": 775, "width": 910, "tabOrder": 5}
    bar_data["visual"]["visualContainerObjects"] = {
        "title": [{
            "properties": {
                "show": {"expr": {"Literal": {"Value": "true"}}},
                "text": {"expr": {"Literal": {"Value": "'Average Length of Stay (Days) by Diagnosis Code'"}}},
                "fontColor": {"solid": {"color": {"expr": {"Literal": {"Value": "'#0F172A'"}}}}},
                "fontSize": {"expr": {"Literal": {"Value": "14D"}}},
                "bold": {"expr": {"Literal": {"Value": "true"}}}
            }
        }],
        "labels": [{
            "properties": {
                "show": {"expr": {"Literal": {"Value": "true"}}},
                "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#1E293B'"}}}}}
            }
        }],
        "background": [{
            "properties": {
                "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#FFFFFF'"}}}}},
                "transparency": {"expr": {"Literal": {"Value": "0D"}}}
            }
        }],
        "border": [{
            "properties": {
                "show": {"expr": {"Literal": {"Value": "true"}}},
                "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#E2E8F0'"}}}}},
                "radius": {"expr": {"Literal": {"Value": "8D"}}}
            }
        }]
    }
    file_entries[bar_key] = json.dumps(bar_data, indent=2).encode("utf-8")
    print("5. Refined Horizontal Bar Chart (Average LOS by Diagnosis Code, data labels ON, clean border).")

    # 6. Donut Chart (87d2b1c1c395dc11ad34): Patient Risk Distribution
    donut_key = "Report/definition/pages/602ddcd746b2159a7a83/visuals/87d2b1c1c395dc11ad34/visual.json"
    donut_data = json.loads(file_entries[donut_key].decode("utf-8"))
    donut_data["position"] = {"x": 965, "y": 275, "z": 6, "height": 365, "width": 925, "tabOrder": 6}
    donut_data["visual"]["visualContainerObjects"] = {
        "title": [{
            "properties": {
                "show": {"expr": {"Literal": {"Value": "true"}}},
                "text": {"expr": {"Literal": {"Value": "'Patient Risk Distribution'"}}},
                "fontColor": {"solid": {"color": {"expr": {"Literal": {"Value": "'#0F172A'"}}}}},
                "fontSize": {"expr": {"Literal": {"Value": "14D"}}},
                "bold": {"expr": {"Literal": {"Value": "true"}}}
            }
        }],
        "labels": [{
            "properties": {
                "show": {"expr": {"Literal": {"Value": "true"}}}
            }
        }],
        "background": [{
            "properties": {
                "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#FFFFFF'"}}}}},
                "transparency": {"expr": {"Literal": {"Value": "0D"}}}
            }
        }],
        "border": [{
            "properties": {
                "show": {"expr": {"Literal": {"Value": "true"}}},
                "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#E2E8F0'"}}}}},
                "radius": {"expr": {"Literal": {"Value": "8D"}}}
            }
        }]
    }
    file_entries[donut_key] = json.dumps(donut_data, indent=2).encode("utf-8")
    print("6. Refined Donut Chart (Patient Risk Distribution, clean labels).")

    # 7. Clustered Column Chart (60588f694c141db6954e): Readmission Risk by Prior Inpatient Admissions
    col_key = "Report/definition/pages/602ddcd746b2159a7a83/visuals/60588f694c141db6954e/visual.json"
    col_data = json.loads(file_entries[col_key].decode("utf-8"))
    col_data["position"] = {"x": 965, "y": 660, "z": 7, "height": 390, "width": 925, "tabOrder": 7}
    col_data["visual"]["visualContainerObjects"] = {
        "title": [{
            "properties": {
                "show": {"expr": {"Literal": {"Value": "true"}}},
                "text": {"expr": {"Literal": {"Value": "'Readmission Risk by Prior Inpatient Admissions'"}}},
                "fontColor": {"solid": {"color": {"expr": {"Literal": {"Value": "'#0F172A'"}}}}},
                "fontSize": {"expr": {"Literal": {"Value": "14D"}}},
                "bold": {"expr": {"Literal": {"Value": "true"}}}
            }
        }],
        "labels": [{
            "properties": {
                "show": {"expr": {"Literal": {"Value": "true"}}}
            }
        }],
        "background": [{
            "properties": {
                "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#FFFFFF'"}}}}},
                "transparency": {"expr": {"Literal": {"Value": "0D"}}}
            }
        }],
        "border": [{
            "properties": {
                "show": {"expr": {"Literal": {"Value": "true"}}},
                "color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#E2E8F0'"}}}}},
                "radius": {"expr": {"Literal": {"Value": "8D"}}}
            }
        }]
    }
    file_entries[col_key] = json.dumps(col_data, indent=2).encode("utf-8")
    print("7. Refined Clustered Column Chart (Readmission Risk by Prior Admissions).")

    # 8. Write Transformed PBIX
    print(f"8. Packaging output to {INPUT_PBIX} and {FINAL_PBIX}...")
    for out_path in [INPUT_PBIX, FINAL_PBIX]:
        with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as z_out:
            for fname, content in file_entries.items():
                z_out.writestr(fname, content)

    print("\n" + "="*74)
    print("🎉 HEALTHCARE MASTER POWER BI TRANSFORMATION SUCCESSFUL!")
    print(f"📁 Updated File: {INPUT_PBIX}")
    print(f"📁 Presentation File: {FINAL_PBIX}")
    print(f"🛡️ Backup File: {BACKUP_PBIX}")
    print("==========================================================================")
    return INPUT_PBIX

if __name__ == "__main__":
    transform_pbir()
