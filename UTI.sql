SELECT
	SPatient.PatientSID
	,SPatient.Sta3n
	,SPatient.PatientSSN
	,SPatient.PatientName
	,Visit.VisitSID
	,CAST(Visit.VisitDateTime AS DATE) AS VisitDate
	,Visit.LocationSID AS VisitLocationSID
	,Loc.LocationName
	,CAST(Inpatient.AdmitDateTime AS DATE) AS AdmitDate
	,CAST(Inpatient.DischargeDateTime AS DATE) AS DischargeDate
	,Ward.WardLocationName
	,Bed.RoomBed
	,SAddress.Zip AS PatientZip
	,SAddress.GISPatientAddressLongitude AS PatientLON
	,SAddress.GISPatientAddressLatitude AS PatientLAT
	,SAddress.GISFIPSCode AS PatientFIPS
	,DimICD.ICD10Description
INTO
	#Temp_BEUTI_1
FROM
	LSV.SPatient.SPatientAddress AS SAddress
	INNER JOIN LSV.SPatient.SPatient AS SPatient
		ON SAddress.PatientSID = SPatient.PatientSID
		AND SPatient.Sta3n = '612'
	INNER JOIN LSV.Outpat.Visit AS Visit
		ON SPatient.PatientSID = Visit.PatientSID
	LEFT JOIN LSV.Dim.Location AS Loc
		ON Visit.LocationSID = Loc.LocationSID
	INNER JOIN LSV.Outpat.VDiagnosis AS VDiagnosis
		ON Visit.VisitSID = VDiagnosis.VisitSID
		AND VDiagnosis.sta3n = '612'
	INNER JOIN LSV.Dim.ICD10DescriptionVersion AS DimICD
		ON VDiagnosis.icd10sid = DimICD.icd10sid
	LEFT JOIN LSV.Inpat.Inpatient AS Inpatient
		ON SPatient.PatientSID = Inpatient.PatientSID
		AND Inpatient.Sta3n = '612'
	LEFT JOIN LSV.Dim.WardLocation AS Ward
		ON Inpatient.AdmitWardLocationSID = Ward.WardLocationSID
		AND Ward.Sta3n = '612'
	LEFT JOIN LSV.Dim.RoomBed AS Bed
		ON Inpatient.AdmitRoomBedSID = Bed.RoomBedSID
		AND Bed.Sta3n = '612'
WHERE
	SAddress.Sta3n = '612'
	AND SAddress.RelationshipToPatient = 'SELF'
	AND SAddress.AddressType = 'PATIENT'
	AND SPatient.TestPatientFlag IS NULL
	AND DimICD.icd10description like '%urinary%'
	AND Visit.VisitDateTime >= DATEADD(MONTH, -24, GETDATE())
	AND ( -- Either not inpatient or 
		(
			Inpatient.AdmitDateTime IS NULL
			AND Visit.VisitDateTime >= DATEADD(MONTH, -24, GETDATE())
		)
		OR (
			Inpatient.AdmitDateTime >= DATEADD(MONTH, -24, GETDATE())
			AND Visit.VisitDateTime BETWEEN Inpatient.AdmitDateTime AND Inpatient.DischargeDateTime
		)
	)
	AND (
		(
			Inpatient.DischargeDateTime IS NULL
			AND Visit.VisitDateTime >= DATEADD(MONTH, -24, GETDATE())
		)
		OR (
			Inpatient.DischargeDateTime >= DATEADD(MONTH, -24, GETDATE())
			AND Visit.VisitDateTime BETWEEN Inpatient.AdmitDateTime AND Inpatient.DischargeDateTime
		)
	)

CREATE CLUSTERED COLUMNSTORE INDEX ccsi_Temp_BEUTI_1
	ON #Temp_BEUTI_1

SELECT
	RxOutpat.PatientSID
	,RxOutpat.Sta3n
	,RxOutpat.PharmacyOrderableItemSID
	,RxOutpat.IssueDate
	,RxOutpat.LocationSID AS RxOutpatLocationSID
	,CAST(Microbiology.SpecimenTakenDateTime AS DATE) AS SpecimenDate
	,RxOutpatFill.DispensedDate
	,RxOutpatFill.LocalDrugNameWithDose
	,RxOutpatFill.qty
	,DimCollectionSample.CollectionSample
	,AntiSens.OrganismQuantity
	,DimAntibiotic.Antibiotic
	,AntiSens.AntibioticSensitivityValue
	,DimAntibiotic.LabProcedure
	,DimOrganism.Organism
INTO
	#Temp_BEUTI_2
FROM
	CDWWork.BISL_R1VX.AR3Y_RxOut_RxOutpat AS RxOutpat
	LEFT JOIN CDWWork.BISL_R1VX.AR3Y_RxOut_RxOutpatfill AS RxOutpatFill
		ON RxOutpat.PatientSID = RxOutpatFill.PatientSID
		AND RxOutpatFill.Sta3n = 612
	INNER JOIN CDWWork.Dim.LocalDrug AS DimLocalDrug
		ON RxOutpatFill.localdrugsid = DimLocalDrug.localdrugsid
		AND DimLocalDrug.Sta3n = 612
	INNER JOIN CDWWork.Micro.Microbiology AS Microbiology
		ON RxOutpat.PatientSID = Microbiology.PatientSID 
		AND Microbiology.Sta3n = 612
	LEFT JOIN CDWWork.Dim.CollectionSample AS DimCollectionSample
		ON Microbiology.CollectionSampleSID = DimCollectionSample.CollectionSampleSID 
		AND DimCollectionSample.Sta3n = 612
	INNER JOIN CDWWork.Micro.AntibioticSensitivity AS AntiSens
		ON Microbiology.MicrobiologySID = AntiSens.MicrobiologySID 
		AND AntiSens.Sta3n = 612
	LEFT JOIN CDWWork.Dim.Antibiotic AS DimAntibiotic
		ON AntiSens.AntibioticSID = DimAntibiotic.AntibioticSID
		AND DimAntibiotic.Sta3n = 612
	LEFT JOIN CDWWork.Dim.Organism AS DimOrganism
		ON AntiSens.OrganismSID = DimOrganism.OrganismSID
		AND DimOrganism.Sta3n = 612
WHERE
	RxOutpat.Sta3n = 612
	AND RxOutpat.IssueDate >= CAST(DATEADD(MONTH, -6, GETDATE()) AS DATE)
	AND Microbiology.SpecimenTakenDateTime >= CAST(DATEADD(MONTH, -6, GETDATE()) AS DATE)
	AND DimLocalDrug.DrugClassSID in ('800010973', '800010982', '800010983', '800010984', '800011263') 

CREATE CLUSTERED COLUMNSTORE INDEX ccsi_Temp_BEUTI_2
	ON #Temp_BEUTI_2

INSERT INTO
	LSV.MAC_UTI
SELECT DISTINCT
	Tbl1.PatientSID
	,Tbl1.Sta3n
	,Tbl1.PatientSSN
	,Tbl1.PatientName
	,Tbl1.VisitDate
	,Tbl1.PatientFIPS
	,Tbl1.AdmitDate
	,Tbl1.DischargeDate
	,Tbl1.WardLocationName
	,Tbl1.RoomBed
	,Tbl1.PatientZip
	,Tbl1.ICD10Description
	,Tbl1.LocationName
	,Tbl1.PatientLON
	,Tbl1.PatientLAT
	,Tbl2.SpecimenDate
	,Tbl2.DispensedDate
	,Tbl2.LocalDrugNameWithDose
	,Tbl2.Qty
	,Tbl2.OrganismQuantity
	,Tbl2.Antibiotic
	,Tbl2.AntibioticSensitivityValue
	,Tbl2.Organism
	,Tbl2.CollectionSample
	,Tbl2.LabProcedure
	,Tbl2.IssueDate
FROM
	#Temp_BEUTI_1 AS tbl1
	INNER JOIN #Temp_BEUTI_2 AS tbl2
		ON tbl1.PatientSID = tbl2.PatientSID
WHERE
	tbl2.SpecimenDate BETWEEN tbl1.VisitDate AND CAST(DATEADD(DAY, +5, tbl1.VisitDate) AS DATE)
	AND tbl2.DispensedDate BETWEEN tbl2.SpecimenDate AND CAST(DATEADD(DAY, +14, tbl2.SpecimenDate) AS DATE)
