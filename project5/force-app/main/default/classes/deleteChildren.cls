trigger deleteChildren on Opportunity (after delete) {
	List<Opportunity> deleted = Trigger.New;
    // iterate each to opportunities to be deleted, and delete all of the children
    // I can't create Master-Detail relationship for standard objects so I create a new field
    // New Filed refers to its parent, type: Lookup, Name: par_Id
    for(Opportunity par : deleted){
        List<Opportunity> children = [Select Id from Opportunity where parId__c =:par.id];
        delete children;
    }
    
}