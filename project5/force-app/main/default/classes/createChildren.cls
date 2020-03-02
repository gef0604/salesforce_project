trigger createChildren on Opportunity (after insert) {
	//new opportunities created
    Opportunity[] pars = Trigger.New;
    //a list to store children
    List<Opportunity> children = new List<Opportunity>();
    
    //iterate each parent
    for(Opportunity par : pars){
        String ins = par.Insurance_Lines__c;
        //if contains the field, create a new one
        if(ins.contains('Life')){
            Opportunity child = new Opportunity();
            child.Insurance_Line__c = 'Life';
            //relationship, from parent to child
            child.parId__c = par.Id;
            children.add(child);
        }
        //if contains the field, create a new one
        if(ins.contains('Health')){
            Opportunity child = new Opportunity();
            child.Insurance_Line__c = 'Health';
            //relationship, from parent to child
            child.parId__c = par.Id;
            children.add(child);
        }
        //if contains the field, create a new one
        if(ins.contains('Dental')){
            Opportunity child = new Opportunity();
            child.Insurance_Line__c = 'Dental';
            //relationship, from parent to child
            child.parId__c = par.Id;
            children.add(child);
        }
        //if contains the field, create a new one
        if(ins.contains('Vision')){
            Opportunity child = new Opportunity();
            child.Insurance_Line__c = 'Vision';
            //relationship, from parent to child
            child.parId__c = par.Id;
            children.add(child);
        }
        
        
    }
    
    	//insert children into database
    insert(children);
    
}