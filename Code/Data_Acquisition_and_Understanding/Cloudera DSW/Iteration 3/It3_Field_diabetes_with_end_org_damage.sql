/* 
New field: Has vs. does not have diabetes with end organ damage 
*/

/*

Problem: LACE includes among comorbidities 'diabetes with end organ damage' I drew a list of corresponding icd10 codes from TK source and found no hit
across all our inpatients. 

Conversation with Leslie, 20180606

- should be included in set of "diabetes with end organ damage": E11.52. At least 1 member has that dx, why was there no hit against my list?
- include all diabetes, exclude from that 'diabetes w/o complication' codes
- if you want to be more refined, exclude primary dx that are likely unrelated (syncopy & collapse; chest pain; sepsis)
- if you want to be more refined, include when another dx is likely a complication from diabetes (acute renal/kidney failure; infection of amputation stump; gastroparesis; heart attack/myocardial infarction (no old mi; and appears as primary dx)

Plan:
- apply logic in creatin 2 fields, a simple one and one that is more refined. Loop in Leslie so she can tinker with this if she has an idea and/or if the field proves powerful. 

*/