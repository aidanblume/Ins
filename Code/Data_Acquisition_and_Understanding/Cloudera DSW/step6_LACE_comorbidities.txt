/***
Title:              step6_LACE_comorbidities
Description:        Add boolean features for each of the LACE comorbidities 
                    Merging priority: QNXT>CLM>ENC
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step5_ER 
Output:             nathalie.prjrea_step6_LACE_comorbidities
***/

/*
COMORBIDITIES TABLE
Reorg data in flatfile.icd10_laceComorbidity_crosswalk so that codes are rows, and comorbidities are columns / boolean features.
*/

drop table if exists nathalie.tmp_comorbidities_refs
;

create table nathalie.tmp_comorbidities_refs as
select code --, comorbid_lace
    , case when comorbid_lace in ('Previous myocardial infarction') then 1 else 0 end as PreviousMyocardialInfarction
    , case when comorbid_lace in ('Cerebrovascular disease') then 1 else 0 end as CerebrovascularDisease
    , case when comorbid_lace in ('Peripheral vascular disease') then 1 else 0 end as PeripheralVascularDisease
    , case when comorbid_lace in ('Diabetes without complications') then 1 else 0 end as DiabetesWithoutComplications
    , case when comorbid_lace in ('Congestive heart failure') then 1 else 0 end as CongestiveHeartFailure
    , case when comorbid_lace in ('Diabetes with end organ damage') then 1 else 0 end as DiabetesWithEndOrganDamage
    , case when comorbid_lace in ('Chronic pulmonary disease') then 1 else 0 end as ChronicPulmonaryDisease
    , case when comorbid_lace in ('Mild liver or renal disease') then 1 else 0 end as MildLiverOrRenalDisease
    , case when comorbid_lace in ('Any tumor (including lymphoma or leukemia)') then 1 else 0 end as AnyTumor
    , case when comorbid_lace in ('Dementia') then 1 else 0 end as Dementia
    , case when comorbid_lace in ('Connective tissue disease') then 1 else 0 end as ConnectiveTissueDisease
    , case when comorbid_lace in ('AIDS') then 1 else 0 end as AIDS
    , case when comorbid_lace in ('Moderate or severe liver or renal disease') then 1 else 0 end as ModerateOrSevereLiverOrRenalDisease
    , case when comorbid_lace in ('Metastatic solid tumor') then 1 else 0 end as MetastaticSolidTumor
from flatfile.icd10_laceComorbidity_crosswalk
;

--Add comobilities indicators

drop table if exists nathalie.prjrea_step6_LACE_comorbidities 
;

create table nathalie.prjrea_step6_LACE_comorbidities 
as
select a.*
    , (case when b1.PreviousMyocardialInfarction is null then 0 else b1.PreviousMyocardialInfarction end) + (case when b2.PreviousMyocardialInfarction is null then 0 else b2.PreviousMyocardialInfarction end)
    + (case when b3.PreviousMyocardialInfarction is null then 0 else b3.PreviousMyocardialInfarction end) + (case when b4.PreviousMyocardialInfarction is null then 0 else b4.PreviousMyocardialInfarction end)
    + (case when b5.PreviousMyocardialInfarction is null then 0 else b5.PreviousMyocardialInfarction end) + (case when b6.PreviousMyocardialInfarction is null then 0 else b6.PreviousMyocardialInfarction end)
    + (case when b7.PreviousMyocardialInfarction is null then 0 else b7.PreviousMyocardialInfarction end) + (case when b8.PreviousMyocardialInfarction is null then 0 else b8.PreviousMyocardialInfarction end)
    + (case when b9.PreviousMyocardialInfarction is null then 0 else b9.PreviousMyocardialInfarction end) + (case when b10.PreviousMyocardialInfarction is null then 0 else b10.PreviousMyocardialInfarction end)
    + (case when b11.PreviousMyocardialInfarction is null then 0 else b11.PreviousMyocardialInfarction end) + (case when b12.PreviousMyocardialInfarction is null then 0 else b12.PreviousMyocardialInfarction end)
    + (case when b13.PreviousMyocardialInfarction is null then 0 else b13.PreviousMyocardialInfarction end) + (case when b14.PreviousMyocardialInfarction is null then 0 else b14.PreviousMyocardialInfarction end)
    + (case when b15.PreviousMyocardialInfarction is null then 0 else b15.PreviousMyocardialInfarction end) + (case when b16.PreviousMyocardialInfarction is null then 0 else b16.PreviousMyocardialInfarction end)
    + (case when b17.PreviousMyocardialInfarction is null then 0 else b17.PreviousMyocardialInfarction end) + (case when b18.PreviousMyocardialInfarction is null then 0 else b18.PreviousMyocardialInfarction end)
    + (case when b19.PreviousMyocardialInfarction is null then 0 else b19.PreviousMyocardialInfarction end) + (case when b20.PreviousMyocardialInfarction is null then 0 else b20.PreviousMyocardialInfarction end)
    + (case when p1.PreviousMyocardialInfarction is null then 0 else p1.PreviousMyocardialInfarction end) + (case when p2.PreviousMyocardialInfarction is null then 0 else p2.PreviousMyocardialInfarction end)
    + (case when p3.PreviousMyocardialInfarction is null then 0 else p3.PreviousMyocardialInfarction end) + (case when p4.PreviousMyocardialInfarction is null then 0 else p4.PreviousMyocardialInfarction end)
    + (case when p5.PreviousMyocardialInfarction is null then 0 else p5.PreviousMyocardialInfarction end) + (case when p6.PreviousMyocardialInfarction is null then 0 else p6.PreviousMyocardialInfarction end)
    + (case when p7.PreviousMyocardialInfarction is null then 0 else p7.PreviousMyocardialInfarction end) + (case when p8.PreviousMyocardialInfarction is null then 0 else p8.PreviousMyocardialInfarction end)
    + (case when p9.PreviousMyocardialInfarction is null then 0 else p9.PreviousMyocardialInfarction end) + (case when p10.PreviousMyocardialInfarction is null then 0 else p10.PreviousMyocardialInfarction end)
    as PreviousMyocardialInfarction
    , (case when b1.CerebrovascularDisease is null then 0 else b1.CerebrovascularDisease end) + (case when b2.CerebrovascularDisease is null then 0 else b2.CerebrovascularDisease end)
    + (case when b3.CerebrovascularDisease is null then 0 else b3.CerebrovascularDisease end) + (case when b4.CerebrovascularDisease is null then 0 else b4.CerebrovascularDisease end)
    + (case when b5.CerebrovascularDisease is null then 0 else b5.CerebrovascularDisease end) + (case when b6.CerebrovascularDisease is null then 0 else b6.CerebrovascularDisease end)
    + (case when b7.CerebrovascularDisease is null then 0 else b7.CerebrovascularDisease end) + (case when b8.CerebrovascularDisease is null then 0 else b8.CerebrovascularDisease end)
    + (case when b9.CerebrovascularDisease is null then 0 else b9.CerebrovascularDisease end) + (case when b10.CerebrovascularDisease is null then 0 else b10.CerebrovascularDisease end)
    + (case when b11.CerebrovascularDisease is null then 0 else b11.CerebrovascularDisease end) + (case when b12.CerebrovascularDisease is null then 0 else b12.CerebrovascularDisease end)
    + (case when b13.CerebrovascularDisease is null then 0 else b13.CerebrovascularDisease end) + (case when b14.CerebrovascularDisease is null then 0 else b14.CerebrovascularDisease end)
    + (case when b15.CerebrovascularDisease is null then 0 else b15.CerebrovascularDisease end) + (case when b16.CerebrovascularDisease is null then 0 else b16.CerebrovascularDisease end)
    + (case when b17.CerebrovascularDisease is null then 0 else b17.CerebrovascularDisease end) + (case when b18.CerebrovascularDisease is null then 0 else b18.CerebrovascularDisease end)
    + (case when b19.CerebrovascularDisease is null then 0 else b19.CerebrovascularDisease end) + (case when b20.CerebrovascularDisease is null then 0 else b20.CerebrovascularDisease end)
    + (case when p1.CerebrovascularDisease is null then 0 else p1.CerebrovascularDisease end) + (case when p2.CerebrovascularDisease is null then 0 else p2.CerebrovascularDisease end)
    + (case when p3.CerebrovascularDisease is null then 0 else p3.CerebrovascularDisease end) + (case when p4.CerebrovascularDisease is null then 0 else p4.CerebrovascularDisease end)
    + (case when p5.CerebrovascularDisease is null then 0 else p5.CerebrovascularDisease end) + (case when p6.CerebrovascularDisease is null then 0 else p6.CerebrovascularDisease end)
    + (case when p7.CerebrovascularDisease is null then 0 else p7.CerebrovascularDisease end) + (case when p8.CerebrovascularDisease is null then 0 else p8.CerebrovascularDisease end)
    + (case when p9.CerebrovascularDisease is null then 0 else p9.CerebrovascularDisease end) + (case when p10.CerebrovascularDisease is null then 0 else p10.CerebrovascularDisease end)
    as CerebrovascularDisease
    , (case when b1.PeripheralVascularDisease is null then 0 else b1.PeripheralVascularDisease end) + (case when b2.PeripheralVascularDisease is null then 0 else b2.PeripheralVascularDisease end)
    + (case when b3.PeripheralVascularDisease is null then 0 else b3.PeripheralVascularDisease end) + (case when b4.PeripheralVascularDisease is null then 0 else b4.PeripheralVascularDisease end)
    + (case when b5.PeripheralVascularDisease is null then 0 else b5.PeripheralVascularDisease end) + (case when b6.PeripheralVascularDisease is null then 0 else b6.PeripheralVascularDisease end)
    + (case when b7.PeripheralVascularDisease is null then 0 else b7.PeripheralVascularDisease end) + (case when b8.PeripheralVascularDisease is null then 0 else b8.PeripheralVascularDisease end)
    + (case when b9.PeripheralVascularDisease is null then 0 else b9.PeripheralVascularDisease end) + (case when b10.PeripheralVascularDisease is null then 0 else b10.PeripheralVascularDisease end)
    + (case when b11.PeripheralVascularDisease is null then 0 else b11.PeripheralVascularDisease end) + (case when b12.PeripheralVascularDisease is null then 0 else b12.PeripheralVascularDisease end)
    + (case when b13.PeripheralVascularDisease is null then 0 else b13.PeripheralVascularDisease end) + (case when b14.PeripheralVascularDisease is null then 0 else b14.PeripheralVascularDisease end)
    + (case when b15.PeripheralVascularDisease is null then 0 else b15.PeripheralVascularDisease end) + (case when b16.PeripheralVascularDisease is null then 0 else b16.PeripheralVascularDisease end)
    + (case when b17.PeripheralVascularDisease is null then 0 else b17.PeripheralVascularDisease end) + (case when b18.PeripheralVascularDisease is null then 0 else b18.PeripheralVascularDisease end)
    + (case when b19.PeripheralVascularDisease is null then 0 else b19.PeripheralVascularDisease end) + (case when b20.PeripheralVascularDisease is null then 0 else b20.PeripheralVascularDisease end)
    + (case when p1.PeripheralVascularDisease is null then 0 else p1.PeripheralVascularDisease end) + (case when p2.PeripheralVascularDisease is null then 0 else p2.PeripheralVascularDisease end)
    + (case when p3.PeripheralVascularDisease is null then 0 else p3.PeripheralVascularDisease end) + (case when p4.PeripheralVascularDisease is null then 0 else p4.PeripheralVascularDisease end)
    + (case when p5.PeripheralVascularDisease is null then 0 else p5.PeripheralVascularDisease end) + (case when p6.PeripheralVascularDisease is null then 0 else p6.PeripheralVascularDisease end)
    + (case when p7.PeripheralVascularDisease is null then 0 else p7.PeripheralVascularDisease end) + (case when p8.PeripheralVascularDisease is null then 0 else p8.PeripheralVascularDisease end)
    + (case when p9.PeripheralVascularDisease is null then 0 else p9.PeripheralVascularDisease end) + (case when p10.PeripheralVascularDisease is null then 0 else p10.PeripheralVascularDisease end)
    as PeripheralVascularDisease
    , (case when b1.DiabetesWithoutComplications is null then 0 else b1.DiabetesWithoutComplications end) + (case when b2.DiabetesWithoutComplications is null then 0 else b2.DiabetesWithoutComplications end)
    + (case when b3.DiabetesWithoutComplications is null then 0 else b3.DiabetesWithoutComplications end) + (case when b4.DiabetesWithoutComplications is null then 0 else b4.DiabetesWithoutComplications end)
    + (case when b5.DiabetesWithoutComplications is null then 0 else b5.DiabetesWithoutComplications end) + (case when b6.DiabetesWithoutComplications is null then 0 else b6.DiabetesWithoutComplications end)
    + (case when b7.DiabetesWithoutComplications is null then 0 else b7.DiabetesWithoutComplications end) + (case when b8.DiabetesWithoutComplications is null then 0 else b8.DiabetesWithoutComplications end)
    + (case when b9.DiabetesWithoutComplications is null then 0 else b9.DiabetesWithoutComplications end) + (case when b10.DiabetesWithoutComplications is null then 0 else b10.DiabetesWithoutComplications end)
    + (case when b11.DiabetesWithoutComplications is null then 0 else b11.DiabetesWithoutComplications end) + (case when b12.DiabetesWithoutComplications is null then 0 else b12.DiabetesWithoutComplications end)
    + (case when b13.DiabetesWithoutComplications is null then 0 else b13.DiabetesWithoutComplications end) + (case when b14.DiabetesWithoutComplications is null then 0 else b14.DiabetesWithoutComplications end)
    + (case when b15.DiabetesWithoutComplications is null then 0 else b15.DiabetesWithoutComplications end) + (case when b16.DiabetesWithoutComplications is null then 0 else b16.DiabetesWithoutComplications end)
    + (case when b17.DiabetesWithoutComplications is null then 0 else b17.DiabetesWithoutComplications end) + (case when b18.DiabetesWithoutComplications is null then 0 else b18.DiabetesWithoutComplications end)
    + (case when b19.DiabetesWithoutComplications is null then 0 else b19.DiabetesWithoutComplications end) + (case when b20.DiabetesWithoutComplications is null then 0 else b20.DiabetesWithoutComplications end)
    + (case when p1.DiabetesWithoutComplications is null then 0 else p1.DiabetesWithoutComplications end) + (case when p2.DiabetesWithoutComplications is null then 0 else p2.DiabetesWithoutComplications end)
    + (case when p3.DiabetesWithoutComplications is null then 0 else p3.DiabetesWithoutComplications end) + (case when p4.DiabetesWithoutComplications is null then 0 else p4.DiabetesWithoutComplications end)
    + (case when p5.DiabetesWithoutComplications is null then 0 else p5.DiabetesWithoutComplications end) + (case when p6.DiabetesWithoutComplications is null then 0 else p6.DiabetesWithoutComplications end)
    + (case when p7.DiabetesWithoutComplications is null then 0 else p7.DiabetesWithoutComplications end) + (case when p8.DiabetesWithoutComplications is null then 0 else p8.DiabetesWithoutComplications end)
    + (case when p9.DiabetesWithoutComplications is null then 0 else p9.DiabetesWithoutComplications end) + (case when p10.DiabetesWithoutComplications is null then 0 else p10.DiabetesWithoutComplications end)
    as DiabetesWithoutComplications
    , (case when b1.CongestiveHeartFailure is null then 0 else b1.CongestiveHeartFailure end) + (case when b2.CongestiveHeartFailure is null then 0 else b2.CongestiveHeartFailure end)
    + (case when b3.CongestiveHeartFailure is null then 0 else b3.CongestiveHeartFailure end) + (case when b4.CongestiveHeartFailure is null then 0 else b4.CongestiveHeartFailure end)
    + (case when b5.CongestiveHeartFailure is null then 0 else b5.CongestiveHeartFailure end) + (case when b6.CongestiveHeartFailure is null then 0 else b6.CongestiveHeartFailure end)
    + (case when b7.CongestiveHeartFailure is null then 0 else b7.CongestiveHeartFailure end) + (case when b8.CongestiveHeartFailure is null then 0 else b8.CongestiveHeartFailure end)
    + (case when b9.CongestiveHeartFailure is null then 0 else b9.CongestiveHeartFailure end) + (case when b10.CongestiveHeartFailure is null then 0 else b10.CongestiveHeartFailure end)
    + (case when b11.CongestiveHeartFailure is null then 0 else b11.CongestiveHeartFailure end) + (case when b12.CongestiveHeartFailure is null then 0 else b12.CongestiveHeartFailure end)
    + (case when b13.CongestiveHeartFailure is null then 0 else b13.CongestiveHeartFailure end) + (case when b14.CongestiveHeartFailure is null then 0 else b14.CongestiveHeartFailure end)
    + (case when b15.CongestiveHeartFailure is null then 0 else b15.CongestiveHeartFailure end) + (case when b16.CongestiveHeartFailure is null then 0 else b16.CongestiveHeartFailure end)
    + (case when b17.CongestiveHeartFailure is null then 0 else b17.CongestiveHeartFailure end) + (case when b18.CongestiveHeartFailure is null then 0 else b18.CongestiveHeartFailure end)
    + (case when b19.CongestiveHeartFailure is null then 0 else b19.CongestiveHeartFailure end) + (case when b20.CongestiveHeartFailure is null then 0 else b20.CongestiveHeartFailure end)
    + (case when p1.CongestiveHeartFailure is null then 0 else p1.CongestiveHeartFailure end) + (case when p2.CongestiveHeartFailure is null then 0 else p2.CongestiveHeartFailure end)
    + (case when p3.CongestiveHeartFailure is null then 0 else p3.CongestiveHeartFailure end) + (case when p4.CongestiveHeartFailure is null then 0 else p4.CongestiveHeartFailure end)
    + (case when p5.CongestiveHeartFailure is null then 0 else p5.CongestiveHeartFailure end) + (case when p6.CongestiveHeartFailure is null then 0 else p6.CongestiveHeartFailure end)
    + (case when p7.CongestiveHeartFailure is null then 0 else p7.CongestiveHeartFailure end) + (case when p8.CongestiveHeartFailure is null then 0 else p8.CongestiveHeartFailure end)
    + (case when p9.CongestiveHeartFailure is null then 0 else p9.CongestiveHeartFailure end) + (case when p10.CongestiveHeartFailure is null then 0 else p10.CongestiveHeartFailure end)
    as CongestiveHeartFailure
    , (case when b1.DiabetesWithEndOrganDamage is null then 0 else b1.DiabetesWithEndOrganDamage end) + (case when b2.DiabetesWithEndOrganDamage is null then 0 else b2.DiabetesWithEndOrganDamage end)
    + (case when b3.DiabetesWithEndOrganDamage is null then 0 else b3.DiabetesWithEndOrganDamage end) + (case when b4.DiabetesWithEndOrganDamage is null then 0 else b4.DiabetesWithEndOrganDamage end)
    + (case when b5.DiabetesWithEndOrganDamage is null then 0 else b5.DiabetesWithEndOrganDamage end) + (case when b6.DiabetesWithEndOrganDamage is null then 0 else b6.DiabetesWithEndOrganDamage end)
    + (case when b7.DiabetesWithEndOrganDamage is null then 0 else b7.DiabetesWithEndOrganDamage end) + (case when b8.DiabetesWithEndOrganDamage is null then 0 else b8.DiabetesWithEndOrganDamage end)
    + (case when b9.DiabetesWithEndOrganDamage is null then 0 else b9.DiabetesWithEndOrganDamage end) + (case when b10.DiabetesWithEndOrganDamage is null then 0 else b10.DiabetesWithEndOrganDamage end)
    + (case when b11.DiabetesWithEndOrganDamage is null then 0 else b11.DiabetesWithEndOrganDamage end) + (case when b12.DiabetesWithEndOrganDamage is null then 0 else b12.DiabetesWithEndOrganDamage end)
    + (case when b13.DiabetesWithEndOrganDamage is null then 0 else b13.DiabetesWithEndOrganDamage end) + (case when b14.DiabetesWithEndOrganDamage is null then 0 else b14.DiabetesWithEndOrganDamage end)
    + (case when b15.DiabetesWithEndOrganDamage is null then 0 else b15.DiabetesWithEndOrganDamage end) + (case when b16.DiabetesWithEndOrganDamage is null then 0 else b16.DiabetesWithEndOrganDamage end)
    + (case when b17.DiabetesWithEndOrganDamage is null then 0 else b17.DiabetesWithEndOrganDamage end) + (case when b18.DiabetesWithEndOrganDamage is null then 0 else b18.DiabetesWithEndOrganDamage end)
    + (case when b19.DiabetesWithEndOrganDamage is null then 0 else b19.DiabetesWithEndOrganDamage end) + (case when b20.DiabetesWithEndOrganDamage is null then 0 else b20.DiabetesWithEndOrganDamage end)
    + (case when p1.DiabetesWithEndOrganDamage is null then 0 else p1.DiabetesWithEndOrganDamage end) + (case when p2.DiabetesWithEndOrganDamage is null then 0 else p2.DiabetesWithEndOrganDamage end)
    + (case when p3.DiabetesWithEndOrganDamage is null then 0 else p3.DiabetesWithEndOrganDamage end) + (case when p4.DiabetesWithEndOrganDamage is null then 0 else p4.DiabetesWithEndOrganDamage end)
    + (case when p5.DiabetesWithEndOrganDamage is null then 0 else p5.DiabetesWithEndOrganDamage end) + (case when p6.DiabetesWithEndOrganDamage is null then 0 else p6.DiabetesWithEndOrganDamage end)
    + (case when p7.DiabetesWithEndOrganDamage is null then 0 else p7.DiabetesWithEndOrganDamage end) + (case when p8.DiabetesWithEndOrganDamage is null then 0 else p8.DiabetesWithEndOrganDamage end)
    + (case when p9.DiabetesWithEndOrganDamage is null then 0 else p9.DiabetesWithEndOrganDamage end) + (case when p10.DiabetesWithEndOrganDamage is null then 0 else p10.DiabetesWithEndOrganDamage end)
    as DiabetesWithEndOrganDamage
    , (case when b1.ChronicPulmonaryDisease is null then 0 else b1.ChronicPulmonaryDisease end) + (case when b2.ChronicPulmonaryDisease is null then 0 else b2.ChronicPulmonaryDisease end)
    + (case when b3.ChronicPulmonaryDisease is null then 0 else b3.ChronicPulmonaryDisease end) + (case when b4.ChronicPulmonaryDisease is null then 0 else b4.ChronicPulmonaryDisease end)
    + (case when b5.ChronicPulmonaryDisease is null then 0 else b5.ChronicPulmonaryDisease end) + (case when b6.ChronicPulmonaryDisease is null then 0 else b6.ChronicPulmonaryDisease end)
    + (case when b7.ChronicPulmonaryDisease is null then 0 else b7.ChronicPulmonaryDisease end) + (case when b8.ChronicPulmonaryDisease is null then 0 else b8.ChronicPulmonaryDisease end)
    + (case when b9.ChronicPulmonaryDisease is null then 0 else b9.ChronicPulmonaryDisease end) + (case when b10.ChronicPulmonaryDisease is null then 0 else b10.ChronicPulmonaryDisease end)
    + (case when b11.ChronicPulmonaryDisease is null then 0 else b11.ChronicPulmonaryDisease end) + (case when b12.ChronicPulmonaryDisease is null then 0 else b12.ChronicPulmonaryDisease end)
    + (case when b13.ChronicPulmonaryDisease is null then 0 else b13.ChronicPulmonaryDisease end) + (case when b14.ChronicPulmonaryDisease is null then 0 else b14.ChronicPulmonaryDisease end)
    + (case when b15.ChronicPulmonaryDisease is null then 0 else b15.ChronicPulmonaryDisease end) + (case when b16.ChronicPulmonaryDisease is null then 0 else b16.ChronicPulmonaryDisease end)
    + (case when b17.ChronicPulmonaryDisease is null then 0 else b17.ChronicPulmonaryDisease end) + (case when b18.ChronicPulmonaryDisease is null then 0 else b18.ChronicPulmonaryDisease end)
    + (case when b19.ChronicPulmonaryDisease is null then 0 else b19.ChronicPulmonaryDisease end) + (case when b20.ChronicPulmonaryDisease is null then 0 else b20.ChronicPulmonaryDisease end)
    + (case when p1.ChronicPulmonaryDisease is null then 0 else p1.ChronicPulmonaryDisease end) + (case when p2.ChronicPulmonaryDisease is null then 0 else p2.ChronicPulmonaryDisease end)
    + (case when p3.ChronicPulmonaryDisease is null then 0 else p3.ChronicPulmonaryDisease end) + (case when p4.ChronicPulmonaryDisease is null then 0 else p4.ChronicPulmonaryDisease end)
    + (case when p5.ChronicPulmonaryDisease is null then 0 else p5.ChronicPulmonaryDisease end) + (case when p6.ChronicPulmonaryDisease is null then 0 else p6.ChronicPulmonaryDisease end)
    + (case when p7.ChronicPulmonaryDisease is null then 0 else p7.ChronicPulmonaryDisease end) + (case when p8.ChronicPulmonaryDisease is null then 0 else p8.ChronicPulmonaryDisease end)
    + (case when p9.ChronicPulmonaryDisease is null then 0 else p9.ChronicPulmonaryDisease end) + (case when p10.ChronicPulmonaryDisease is null then 0 else p10.ChronicPulmonaryDisease end)
    as ChronicPulmonaryDisease
    , (case when b1.MildLiverOrRenalDisease is null then 0 else b1.MildLiverOrRenalDisease end) + (case when b2.MildLiverOrRenalDisease is null then 0 else b2.MildLiverOrRenalDisease end)
    + (case when b3.MildLiverOrRenalDisease is null then 0 else b3.MildLiverOrRenalDisease end) + (case when b4.MildLiverOrRenalDisease is null then 0 else b4.MildLiverOrRenalDisease end)
    + (case when b5.MildLiverOrRenalDisease is null then 0 else b5.MildLiverOrRenalDisease end) + (case when b6.MildLiverOrRenalDisease is null then 0 else b6.MildLiverOrRenalDisease end)
    + (case when b7.MildLiverOrRenalDisease is null then 0 else b7.MildLiverOrRenalDisease end) + (case when b8.MildLiverOrRenalDisease is null then 0 else b8.MildLiverOrRenalDisease end)
    + (case when b9.MildLiverOrRenalDisease is null then 0 else b9.MildLiverOrRenalDisease end) + (case when b10.MildLiverOrRenalDisease is null then 0 else b10.MildLiverOrRenalDisease end)
    + (case when b11.MildLiverOrRenalDisease is null then 0 else b11.MildLiverOrRenalDisease end) + (case when b12.MildLiverOrRenalDisease is null then 0 else b12.MildLiverOrRenalDisease end)
    + (case when b13.MildLiverOrRenalDisease is null then 0 else b13.MildLiverOrRenalDisease end) + (case when b14.MildLiverOrRenalDisease is null then 0 else b14.MildLiverOrRenalDisease end)
    + (case when b15.MildLiverOrRenalDisease is null then 0 else b15.MildLiverOrRenalDisease end) + (case when b16.MildLiverOrRenalDisease is null then 0 else b16.MildLiverOrRenalDisease end)
    + (case when b17.MildLiverOrRenalDisease is null then 0 else b17.MildLiverOrRenalDisease end) + (case when b18.MildLiverOrRenalDisease is null then 0 else b18.MildLiverOrRenalDisease end)
    + (case when b19.MildLiverOrRenalDisease is null then 0 else b19.MildLiverOrRenalDisease end) + (case when b20.MildLiverOrRenalDisease is null then 0 else b20.MildLiverOrRenalDisease end)
    + (case when p1.MildLiverOrRenalDisease is null then 0 else p1.MildLiverOrRenalDisease end) + (case when p2.MildLiverOrRenalDisease is null then 0 else p2.MildLiverOrRenalDisease end)
    + (case when p3.MildLiverOrRenalDisease is null then 0 else p3.MildLiverOrRenalDisease end) + (case when p4.MildLiverOrRenalDisease is null then 0 else p4.MildLiverOrRenalDisease end)
    + (case when p5.MildLiverOrRenalDisease is null then 0 else p5.MildLiverOrRenalDisease end) + (case when p6.MildLiverOrRenalDisease is null then 0 else p6.MildLiverOrRenalDisease end)
    + (case when p7.MildLiverOrRenalDisease is null then 0 else p7.MildLiverOrRenalDisease end) + (case when p8.MildLiverOrRenalDisease is null then 0 else p8.MildLiverOrRenalDisease end)
    + (case when p9.MildLiverOrRenalDisease is null then 0 else p9.MildLiverOrRenalDisease end) + (case when p10.MildLiverOrRenalDisease is null then 0 else p10.MildLiverOrRenalDisease end)
    as MildLiverOrRenalDisease
    , (case when b1.AnyTumor is null then 0 else b1.AnyTumor end) + (case when b2.AnyTumor is null then 0 else b2.AnyTumor end)
    + (case when b3.AnyTumor is null then 0 else b3.AnyTumor end) + (case when b4.AnyTumor is null then 0 else b4.AnyTumor end)
    + (case when b5.AnyTumor is null then 0 else b5.AnyTumor end) + (case when b6.AnyTumor is null then 0 else b6.AnyTumor end)
    + (case when b7.AnyTumor is null then 0 else b7.AnyTumor end) + (case when b8.AnyTumor is null then 0 else b8.AnyTumor end)
    + (case when b9.AnyTumor is null then 0 else b9.AnyTumor end) + (case when b10.AnyTumor is null then 0 else b10.AnyTumor end)
    + (case when b11.AnyTumor is null then 0 else b11.AnyTumor end) + (case when b12.AnyTumor is null then 0 else b12.AnyTumor end)
    + (case when b13.AnyTumor is null then 0 else b13.AnyTumor end) + (case when b14.AnyTumor is null then 0 else b14.AnyTumor end)
    + (case when b15.AnyTumor is null then 0 else b15.AnyTumor end) + (case when b16.AnyTumor is null then 0 else b16.AnyTumor end)
    + (case when b17.AnyTumor is null then 0 else b17.AnyTumor end) + (case when b18.AnyTumor is null then 0 else b18.AnyTumor end)
    + (case when b19.AnyTumor is null then 0 else b19.AnyTumor end) + (case when b20.AnyTumor is null then 0 else b20.AnyTumor end)
    + (case when p1.AnyTumor is null then 0 else p1.AnyTumor end) + (case when p2.AnyTumor is null then 0 else p2.AnyTumor end)
    + (case when p3.AnyTumor is null then 0 else p3.AnyTumor end) + (case when p4.AnyTumor is null then 0 else p4.AnyTumor end)
    + (case when p5.AnyTumor is null then 0 else p5.AnyTumor end) + (case when p6.AnyTumor is null then 0 else p6.AnyTumor end)
    + (case when p7.AnyTumor is null then 0 else p7.AnyTumor end) + (case when p8.AnyTumor is null then 0 else p8.AnyTumor end)
    + (case when p9.AnyTumor is null then 0 else p9.AnyTumor end) + (case when p10.AnyTumor is null then 0 else p10.AnyTumor end)
    as AnyTumor
    , (case when b1.Dementia is null then 0 else b1.Dementia end) + (case when b2.Dementia is null then 0 else b2.Dementia end)
    + (case when b3.Dementia is null then 0 else b3.Dementia end) + (case when b4.Dementia is null then 0 else b4.Dementia end)
    + (case when b5.Dementia is null then 0 else b5.Dementia end) + (case when b6.Dementia is null then 0 else b6.Dementia end)
    + (case when b7.Dementia is null then 0 else b7.Dementia end) + (case when b8.Dementia is null then 0 else b8.Dementia end)
    + (case when b9.Dementia is null then 0 else b9.Dementia end) + (case when b10.Dementia is null then 0 else b10.Dementia end)
    + (case when b11.Dementia is null then 0 else b11.Dementia end) + (case when b12.Dementia is null then 0 else b12.Dementia end)
    + (case when b13.Dementia is null then 0 else b13.Dementia end) + (case when b14.Dementia is null then 0 else b14.Dementia end)
    + (case when b15.Dementia is null then 0 else b15.Dementia end) + (case when b16.Dementia is null then 0 else b16.Dementia end)
    + (case when b17.Dementia is null then 0 else b17.Dementia end) + (case when b18.Dementia is null then 0 else b18.Dementia end)
    + (case when b19.Dementia is null then 0 else b19.Dementia end) + (case when b20.Dementia is null then 0 else b20.Dementia end)
    + (case when p1.Dementia is null then 0 else p1.Dementia end) + (case when p2.Dementia is null then 0 else p2.Dementia end)
    + (case when p3.Dementia is null then 0 else p3.Dementia end) + (case when p4.Dementia is null then 0 else p4.Dementia end)
    + (case when p5.Dementia is null then 0 else p5.Dementia end) + (case when p6.Dementia is null then 0 else p6.Dementia end)
    + (case when p7.Dementia is null then 0 else p7.Dementia end) + (case when p8.Dementia is null then 0 else p8.Dementia end)
    + (case when p9.Dementia is null then 0 else p9.Dementia end) + (case when p10.Dementia is null then 0 else p10.Dementia end)
    as Dementia
    , (case when b1.ConnectiveTissueDisease is null then 0 else b1.ConnectiveTissueDisease end) + (case when b2.ConnectiveTissueDisease is null then 0 else b2.ConnectiveTissueDisease end)
    + (case when b3.ConnectiveTissueDisease is null then 0 else b3.ConnectiveTissueDisease end) + (case when b4.ConnectiveTissueDisease is null then 0 else b4.ConnectiveTissueDisease end)
    + (case when b5.ConnectiveTissueDisease is null then 0 else b5.ConnectiveTissueDisease end) + (case when b6.ConnectiveTissueDisease is null then 0 else b6.ConnectiveTissueDisease end)
    + (case when b7.ConnectiveTissueDisease is null then 0 else b7.ConnectiveTissueDisease end) + (case when b8.ConnectiveTissueDisease is null then 0 else b8.ConnectiveTissueDisease end)
    + (case when b9.ConnectiveTissueDisease is null then 0 else b9.ConnectiveTissueDisease end) + (case when b10.ConnectiveTissueDisease is null then 0 else b10.ConnectiveTissueDisease end)
    + (case when b11.ConnectiveTissueDisease is null then 0 else b11.ConnectiveTissueDisease end) + (case when b12.ConnectiveTissueDisease is null then 0 else b12.ConnectiveTissueDisease end)
    + (case when b13.ConnectiveTissueDisease is null then 0 else b13.ConnectiveTissueDisease end) + (case when b14.ConnectiveTissueDisease is null then 0 else b14.ConnectiveTissueDisease end)
    + (case when b15.ConnectiveTissueDisease is null then 0 else b15.ConnectiveTissueDisease end) + (case when b16.ConnectiveTissueDisease is null then 0 else b16.ConnectiveTissueDisease end)
    + (case when b17.ConnectiveTissueDisease is null then 0 else b17.ConnectiveTissueDisease end) + (case when b18.ConnectiveTissueDisease is null then 0 else b18.ConnectiveTissueDisease end)
    + (case when b19.ConnectiveTissueDisease is null then 0 else b19.ConnectiveTissueDisease end) + (case when b20.ConnectiveTissueDisease is null then 0 else b20.ConnectiveTissueDisease end)
    + (case when p1.ConnectiveTissueDisease is null then 0 else p1.ConnectiveTissueDisease end) + (case when p2.ConnectiveTissueDisease is null then 0 else p2.ConnectiveTissueDisease end)
    + (case when p3.ConnectiveTissueDisease is null then 0 else p3.ConnectiveTissueDisease end) + (case when p4.ConnectiveTissueDisease is null then 0 else p4.ConnectiveTissueDisease end)
    + (case when p5.ConnectiveTissueDisease is null then 0 else p5.ConnectiveTissueDisease end) + (case when p6.ConnectiveTissueDisease is null then 0 else p6.ConnectiveTissueDisease end)
    + (case when p7.ConnectiveTissueDisease is null then 0 else p7.ConnectiveTissueDisease end) + (case when p8.ConnectiveTissueDisease is null then 0 else p8.ConnectiveTissueDisease end)
    + (case when p9.ConnectiveTissueDisease is null then 0 else p9.ConnectiveTissueDisease end) + (case when p10.ConnectiveTissueDisease is null then 0 else p10.ConnectiveTissueDisease end)
    as ConnectiveTissueDisease
    , (case when b1.AIDS is null then 0 else b1.AIDS end) + (case when b2.AIDS is null then 0 else b2.AIDS end)
    + (case when b3.AIDS is null then 0 else b3.AIDS end) + (case when b4.AIDS is null then 0 else b4.AIDS end)
    + (case when b5.AIDS is null then 0 else b5.AIDS end) + (case when b6.AIDS is null then 0 else b6.AIDS end)
    + (case when b7.AIDS is null then 0 else b7.AIDS end) + (case when b8.AIDS is null then 0 else b8.AIDS end)
    + (case when b9.AIDS is null then 0 else b9.AIDS end) + (case when b10.AIDS is null then 0 else b10.AIDS end)
    + (case when b11.AIDS is null then 0 else b11.AIDS end) + (case when b12.AIDS is null then 0 else b12.AIDS end)
    + (case when b13.AIDS is null then 0 else b13.AIDS end) + (case when b14.AIDS is null then 0 else b14.AIDS end)
    + (case when b15.AIDS is null then 0 else b15.AIDS end) + (case when b16.AIDS is null then 0 else b16.AIDS end)
    + (case when b17.AIDS is null then 0 else b17.AIDS end) + (case when b18.AIDS is null then 0 else b18.AIDS end)
    + (case when b19.AIDS is null then 0 else b19.AIDS end) + (case when b20.AIDS is null then 0 else b20.AIDS end)
    + (case when p1.AIDS is null then 0 else p1.AIDS end) + (case when p2.AIDS is null then 0 else p2.AIDS end)
    + (case when p3.AIDS is null then 0 else p3.AIDS end) + (case when p4.AIDS is null then 0 else p4.AIDS end)
    + (case when p5.AIDS is null then 0 else p5.AIDS end) + (case when p6.AIDS is null then 0 else p6.AIDS end)
    + (case when p7.AIDS is null then 0 else p7.AIDS end) + (case when p8.AIDS is null then 0 else p8.AIDS end)
    + (case when p9.AIDS is null then 0 else p9.AIDS end) + (case when p10.AIDS is null then 0 else p10.AIDS end)
    as AIDS
    , (case when b1.ModerateOrSevereLiverOrRenalDisease is null then 0 else b1.ModerateOrSevereLiverOrRenalDisease end) + (case when b2.ModerateOrSevereLiverOrRenalDisease is null then 0 else b2.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b3.ModerateOrSevereLiverOrRenalDisease is null then 0 else b3.ModerateOrSevereLiverOrRenalDisease end) + (case when b4.ModerateOrSevereLiverOrRenalDisease is null then 0 else b4.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b5.ModerateOrSevereLiverOrRenalDisease is null then 0 else b5.ModerateOrSevereLiverOrRenalDisease end) + (case when b6.ModerateOrSevereLiverOrRenalDisease is null then 0 else b6.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b7.ModerateOrSevereLiverOrRenalDisease is null then 0 else b7.ModerateOrSevereLiverOrRenalDisease end) + (case when b8.ModerateOrSevereLiverOrRenalDisease is null then 0 else b8.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b9.ModerateOrSevereLiverOrRenalDisease is null then 0 else b9.ModerateOrSevereLiverOrRenalDisease end) + (case when b10.ModerateOrSevereLiverOrRenalDisease is null then 0 else b10.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b11.ModerateOrSevereLiverOrRenalDisease is null then 0 else b11.ModerateOrSevereLiverOrRenalDisease end) + (case when b12.ModerateOrSevereLiverOrRenalDisease is null then 0 else b12.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b13.ModerateOrSevereLiverOrRenalDisease is null then 0 else b13.ModerateOrSevereLiverOrRenalDisease end) + (case when b14.ModerateOrSevereLiverOrRenalDisease is null then 0 else b14.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b15.ModerateOrSevereLiverOrRenalDisease is null then 0 else b15.ModerateOrSevereLiverOrRenalDisease end) + (case when b16.ModerateOrSevereLiverOrRenalDisease is null then 0 else b16.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b17.ModerateOrSevereLiverOrRenalDisease is null then 0 else b17.ModerateOrSevereLiverOrRenalDisease end) + (case when b18.ModerateOrSevereLiverOrRenalDisease is null then 0 else b18.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b19.ModerateOrSevereLiverOrRenalDisease is null then 0 else b19.ModerateOrSevereLiverOrRenalDisease end) + (case when b20.ModerateOrSevereLiverOrRenalDisease is null then 0 else b20.ModerateOrSevereLiverOrRenalDisease end)
    + (case when p1.ModerateOrSevereLiverOrRenalDisease is null then 0 else p1.ModerateOrSevereLiverOrRenalDisease end) + (case when p2.ModerateOrSevereLiverOrRenalDisease is null then 0 else p2.ModerateOrSevereLiverOrRenalDisease end)
    + (case when p3.ModerateOrSevereLiverOrRenalDisease is null then 0 else p3.ModerateOrSevereLiverOrRenalDisease end) + (case when p4.ModerateOrSevereLiverOrRenalDisease is null then 0 else p4.ModerateOrSevereLiverOrRenalDisease end)
    + (case when p5.ModerateOrSevereLiverOrRenalDisease is null then 0 else p5.ModerateOrSevereLiverOrRenalDisease end) + (case when p6.ModerateOrSevereLiverOrRenalDisease is null then 0 else p6.ModerateOrSevereLiverOrRenalDisease end)
    + (case when p7.ModerateOrSevereLiverOrRenalDisease is null then 0 else p7.ModerateOrSevereLiverOrRenalDisease end) + (case when p8.ModerateOrSevereLiverOrRenalDisease is null then 0 else p8.ModerateOrSevereLiverOrRenalDisease end)
    + (case when p9.ModerateOrSevereLiverOrRenalDisease is null then 0 else p9.ModerateOrSevereLiverOrRenalDisease end) + (case when p10.ModerateOrSevereLiverOrRenalDisease is null then 0 else p10.ModerateOrSevereLiverOrRenalDisease end)
    as ModerateOrSevereLiverOrRenalDisease
    , (case when b1.MetastaticSolidTumor is null then 0 else b1.MetastaticSolidTumor end) + (case when b2.MetastaticSolidTumor is null then 0 else b2.MetastaticSolidTumor end)
    + (case when b3.MetastaticSolidTumor is null then 0 else b3.MetastaticSolidTumor end) + (case when b4.MetastaticSolidTumor is null then 0 else b4.MetastaticSolidTumor end)
    + (case when b5.MetastaticSolidTumor is null then 0 else b5.MetastaticSolidTumor end) + (case when b6.MetastaticSolidTumor is null then 0 else b6.MetastaticSolidTumor end)
    + (case when b7.MetastaticSolidTumor is null then 0 else b7.MetastaticSolidTumor end) + (case when b8.MetastaticSolidTumor is null then 0 else b8.MetastaticSolidTumor end)
    + (case when b9.MetastaticSolidTumor is null then 0 else b9.MetastaticSolidTumor end) + (case when b10.MetastaticSolidTumor is null then 0 else b10.MetastaticSolidTumor end)
    + (case when b11.MetastaticSolidTumor is null then 0 else b11.MetastaticSolidTumor end) + (case when b12.MetastaticSolidTumor is null then 0 else b12.MetastaticSolidTumor end)
    + (case when b13.MetastaticSolidTumor is null then 0 else b13.MetastaticSolidTumor end) + (case when b14.MetastaticSolidTumor is null then 0 else b14.MetastaticSolidTumor end)
    + (case when b15.MetastaticSolidTumor is null then 0 else b15.MetastaticSolidTumor end) + (case when b16.MetastaticSolidTumor is null then 0 else b16.MetastaticSolidTumor end)
    + (case when b17.MetastaticSolidTumor is null then 0 else b17.MetastaticSolidTumor end) + (case when b18.MetastaticSolidTumor is null then 0 else b18.MetastaticSolidTumor end)
    + (case when b19.MetastaticSolidTumor is null then 0 else b19.MetastaticSolidTumor end) + (case when b20.MetastaticSolidTumor is null then 0 else b20.MetastaticSolidTumor end)
    + (case when p1.MetastaticSolidTumor is null then 0 else p1.MetastaticSolidTumor end) + (case when p2.MetastaticSolidTumor is null then 0 else p2.MetastaticSolidTumor end)
    + (case when p3.MetastaticSolidTumor is null then 0 else p3.MetastaticSolidTumor end) + (case when p4.MetastaticSolidTumor is null then 0 else p4.MetastaticSolidTumor end)
    + (case when p5.MetastaticSolidTumor is null then 0 else p5.MetastaticSolidTumor end) + (case when p6.MetastaticSolidTumor is null then 0 else p6.MetastaticSolidTumor end)
    + (case when p7.MetastaticSolidTumor is null then 0 else p7.MetastaticSolidTumor end) + (case when p8.MetastaticSolidTumor is null then 0 else p8.MetastaticSolidTumor end)
    + (case when p9.MetastaticSolidTumor is null then 0 else p9.MetastaticSolidTumor end) + (case when p10.MetastaticSolidTumor is null then 0 else p10.MetastaticSolidTumor end)
    as MetastaticSolidTumor
from NATHALIE.prjrea_step5_ER a
left join NATHALIE.tmp_comorbidities_refs b1 on a.case_dx1=b1.code
left join NATHALIE.tmp_comorbidities_refs b2 on a.case_dx2=b2.code
left join NATHALIE.tmp_comorbidities_refs b3 on a.case_dx3=b3.code
left join NATHALIE.tmp_comorbidities_refs b4 on a.case_dx4=b4.code
left join NATHALIE.tmp_comorbidities_refs b5 on a.case_dx5=b5.code
left join NATHALIE.tmp_comorbidities_refs b6 on a.case_dx6=b6.code
left join NATHALIE.tmp_comorbidities_refs b7 on a.case_dx7=b7.code
left join NATHALIE.tmp_comorbidities_refs b8 on a.case_dx8=b8.code
left join NATHALIE.tmp_comorbidities_refs b9 on a.case_dx9=b9.code
left join NATHALIE.tmp_comorbidities_refs b10 on a.case_dx10=b10.code
left join NATHALIE.tmp_comorbidities_refs b11 on a.case_dx11=b11.code
left join NATHALIE.tmp_comorbidities_refs b12 on a.case_dx12=b12.code
left join NATHALIE.tmp_comorbidities_refs b13 on a.case_dx13=b13.code
left join NATHALIE.tmp_comorbidities_refs b14 on a.case_dx14=b14.code
left join NATHALIE.tmp_comorbidities_refs b15 on a.case_dx15=b15.code
left join NATHALIE.tmp_comorbidities_refs b16 on a.case_dx16=b16.code
left join NATHALIE.tmp_comorbidities_refs b17 on a.case_dx17=b17.code
left join NATHALIE.tmp_comorbidities_refs b18 on a.case_dx18=b18.code
left join NATHALIE.tmp_comorbidities_refs b19 on a.case_dx19=b19.code
left join NATHALIE.tmp_comorbidities_refs b20 on a.case_dx20=b20.code
left join NATHALIE.tmp_comorbidities_refs p1 on a.case_pr1=p1.code
left join NATHALIE.tmp_comorbidities_refs p2 on a.case_pr2=p2.code
left join NATHALIE.tmp_comorbidities_refs p3 on a.case_pr3=p3.code
left join NATHALIE.tmp_comorbidities_refs p4 on a.case_pr4=p4.code
left join NATHALIE.tmp_comorbidities_refs p5 on a.case_pr5=p5.code
left join NATHALIE.tmp_comorbidities_refs p6 on a.case_pr6=p6.code
left join NATHALIE.tmp_comorbidities_refs p7 on a.case_pr7=p7.code
left join NATHALIE.tmp_comorbidities_refs p8 on a.case_pr8=p8.code
left join NATHALIE.tmp_comorbidities_refs p9 on a.case_pr9=p9.code
left join NATHALIE.tmp_comorbidities_refs p10 on a.case_pr10=p10.code
;

-- -- Check results;
-- select count(*)
--         -- , sum(case when Pregnancy>0 then 1 else 0 end) as Pregnancy
--         -- , sum(case when Chemo>0 then 1 else 0 end) as Chemo
--         -- , sum(case when Rehab>0 then 1 else 0 end) as Rehab
--         -- , sum(case when Transplant>0 then 1 else 0 end) as Transplant
--         , sum(case when PreviousMyocardialInfarction>0 then 1 else 0 end) as PreviousMyocardialInfarction
--         , sum(case when CerebrovascularDisease>0 then 1 else 0 end) as CerebrovascularDisease
--         , sum(case when PeripheralVascularDisease>0 then 1 else 0 end) as PeripheralVascularDisease
--         , sum(case when DiabetesWithoutComplications>0 then 1 else 0 end) as DiabetesWithoutComplications
--         , sum(case when CongestiveHeartFailure>0 then 1 else 0 end) as CongestiveHeartFailure
--         , sum(case when DiabetesWithEndOrganDamage>0 then 1 else 0 end) as DiabetesWithEndOrganDamage
--         , sum(case when ChronicPulmonaryDisease>0 then 1 else 0 end) as ChronicPulmonaryDisease
--         , sum(case when MildLiverOrRenalDisease>0 then 1 else 0 end) as MildLiverOrRenalDisease
--         , sum(case when AnyTumor>0 then 1 else 0 end) as AnyTumor
--         , sum(case when Dementia>0 then 1 else 0 end) as Dementia
--         , sum(case when ConnectiveTissueDisease>0 then 1 else 0 end) as ConnectiveTissueDisease
--         , sum(case when AIDS>0 then 1 else 0 end) as AIDS
--         , sum(case when ModerateOrSevereLiverOrRenalDisease>0 then 1 else 0 end) as ModerateOrSevereLiverOrRenalDisease
--         , sum(case when MetastaticSolidTumor>0 then 1 else 0 end) as MetastaticSolidTumor
-- from nathalie.prjrea_step6b_LACE_comorbidities;

/*
CLEAN UP
*/

drop table if exists nathalie.tmp_comorbidities_refs;
