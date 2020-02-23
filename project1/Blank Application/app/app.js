import { LightningElement, track, api } from 'lwc';

export default class App extends LightningElement {
@track accs = [
    {
        id : 1,
        Name : "Gef",
        Industry : "IT",
        Sector : "Development",
        Phone : "6822832318",
    },
];
@track newName = "";
@track newIndustry = "";
@track newSector = "";
@track newPhone = "";
@track idGenerater = 2;

handleChange(event){
    const field = event.target.name;
    if(field === "newName"){
        this.newName = event.target.value;
    }else if(field === "newIndustry"){
        this.newIndustry = event.target.value;
    }else if(field === "newSector"){
        this.newSector = event.target.value;
    }else if(field === "newPhone"){
        this.newPhone = event.target.value;
    }    
}

// wait enter to insert a new element in account list

handleKeyPress(event){
    if(event.keyCode === 13){
        var curId = this.idGenerater;
 
        this.accs.push(
            {
            id : curId,
            Name : this.newName,
            Industry : this.newIndustry,
            Sector : this.newSector,
            Phone : this.newPhone
            }   
        );
        this.idGenerater++;
        this.newName = "";
        this.newIndustry = "";
        this.newPhone = "";
        this.newSector = "";
    }

}
}
