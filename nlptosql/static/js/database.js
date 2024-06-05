var checkedValues = [];
const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]')
const tooltipList = [...tooltipTriggerList].map(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl))
var mdata={}
var colorNames = [
    "Orange", "Red", "Lime green","Pink", "Lemon yellow", "Lemon yellow", "Maroon","Yellow", "Sky blue", "Salmon",
    "Green",  "Crimson", "Aqua", "Grey", "Purple", "Mustard", "Peach",
    "Lemon yellow", "Magenta", "Coral", "Saffron", "Brown",  "Tan", "Teal","Black",
    "Navy Blue", "Turquoise", "Lavender", "Beige", "Grape vine", "Indigo", "Fuchsia",
    "Amber", "Sea green", "Dark green", "Burgundy", "Charcoal", "Bronze", "Cream", "Mauve",
    "Olive", "Cyan", "Silver", "Rust", "Ruby", "Azure", "Mint", "Pearl",
    "Ivory", "Tangerine", "Cherry red", "Garnet", "Emerald", "Sapphire", "Rosewood", "Lilac",
    "Arctic blue", "Pista green", "Coffee brown", "Umber", "Brunette", "Mocha", "Ash", "Jet black"
  ];

$(document).ready(function() {
    // AJAX call when the document is ready
    $.ajax({
      url: 'getmetadata', // Replace with your API endpoint
      method: 'GET',
      success: function(data) { 
        console.log(data);
        if(data !='nothing'){
            $("#hostname").val(data['metadata']["db_host"])
            $("#user").val(data['metadata']["db_user"])
            $("#password1").val(data['metadata']["db_password"])
            $("#portno").val(data['metadata']["db_port"])
            $("#database").val(data['metadata']["db_name"])
            $(".latestconn").show()
            $(".latdb").html(`Mssql <button class="btn btn-sm btn-danger float-end " onclick="disconnect()">Disconnect</button>`)
            $(".latdbusername").text($("#user").val())
            $(".latdbname").text($("#database").val())
            // $("#showmf").show()
            mdata=JSON.parse(data['metadata']['schema'])
            console.log('getmdata',typeof(mdata));
            createtablehtml(data['schema'])
        }
      },
      error: function(xhr, status, error) {

      }
    });
  });

$(".connectmysql").click((e)=>{
    $("#append-db-error").html('')
    $("#sloader1").show()
    e.preventDefault()

    var form = document.getElementById('db-form');
    var inputs = form.querySelectorAll('.form-control');
    var isValid = true;

    inputs.forEach(function(input) {
      if (input.value.trim() === '') {
        input.classList.add('is-invalid');
        isValid = false;
      } else {
        input.classList.remove('is-invalid');
      }
    });

    if (isValid) {
        const formdata=new FormData()
        formdata.append('hostname', $("#hostname").val())
        formdata.append('user',$("#user").val())
        formdata.append('password',$("#password1").val())
        formdata.append('portno',$("#portno").val())
        formdata.append('database',$("#database").val())
        $.ajax({
            url: '/connectdb', 
            method: 'POST',
            data:formdata,
            processData: false, 
            contentType: false, 
            success: function(data) {
                console.log(data);
                if(data.msg != 'error'){
                    // checkedValues=[]
                    mdata={}
                    createtablehtml(data.schema)
                    let ss=`<button class="btn btn-primary btn-sm float-start" onclick="selectall()">Select All <i class="bi bi-check2-square"></i></button>`
                    let message='Database Connection successful !'
                    let type='info'
                    let div='append-db-error'
                    appendAlert(message, type,div)
                    // setTimeout(()=>{$("#nav-tbl-tab").click() },2000)
                    $(".telldb").text('Mysql'.toUpperCase())
                    $(".telldbname").text($("#database").val().toUpperCase())
                    $("#showmf").show()
                    $(".showconnect").show()
                    $(".latestconn").show()
                    $(".latdb").html(`Mssql <button class="btn btn-sm btn-danger float-end " onclick="disconnect()">Disconnect</button>`)
                    $(".latdbusername").text($("#user").val())
                    $(".latdbname").text($("#database").val())
                    
                }else{
                    let message='Database Connection Error !'
                    let type='danger'
                    let div='append-db-error'
                    appendAlert(message, type,div)
                }
                $("#sloader1").hide() 
            },error: function(error) {
                $("#sloader1").hide()
                let message='Database Connection Error !'
                let type='danger'
                let div='append-db-error'
                appendAlert(message, type,div)
            console.log(error);
            }
        });
    }else{
        $("#sloader1").hide()
    }
})

function hasKey(obj, key) {
    for (let prop in obj) {
        if (obj.hasOwnProperty(prop)) {
            if (prop === key) {
                return true; // If the key exists in the object, return true
            }
            if (typeof obj[prop] === 'object' && hasKey(obj[prop], key)) {
                return true; // If the key exists in a nested object, return true
            }
        }
    }
    return false; // If the key is not found, return false
}

// let skipdb=["master" ,"model","msdb","tempdb"]
function createtablehtml(data){
     console.log(data);
     document.getElementsByClassName('db-list')[0].innerHTML=''
     let htmlstr=`<ul class="ps-3">` 
     // console.log(data); 
     let dbnames=Object.keys(data)
 //    console.log(dbnames);
     for(let db=0;db<dbnames.length;db++){   //get all database names 
     //    console.log(dbnames[db]);
         htmlstr+=`<li >
             <span class="m-symbol" onclick="toggleview(this)">
                 <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-plus" viewBox="0 0 16 16">
                 <path d="M8 4a.5.5 0 0 1 .5.5v3h3a.5.5 0 0 1 0 1h-3v3a.5.5 0 0 1-1 0v-3h-3a.5.5 0 0 1 0-1h3v-3A.5.5 0 0 1 8 4"/>
                 </svg>
             </span>
             <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-database-fill" viewBox="0 0 16 16">
                 <path d="M3.904 1.777C4.978 1.289 6.427 1 8 1s3.022.289 4.096.777C13.125 2.245 14 2.993 14 4s-.875 1.755-1.904 2.223C11.022 6.711 9.573 7 8 7s-3.022-.289-4.096-.777C2.875 5.755 2 5.007 2 4s.875-1.755 1.904-2.223"/>
                 <path d="M2 6.161V7c0 1.007.875 1.755 1.904 2.223C4.978 9.71 6.427 10 8 10s3.022-.289 4.096-.777C13.125 8.755 14 8.007 14 7v-.839c-.457.432-1.004.751-1.49.972C11.278 7.693 9.682 8 8 8s-3.278-.307-4.51-.867c-.486-.22-1.033-.54-1.49-.972"/>
                 <path d="M2 9.161V10c0 1.007.875 1.755 1.904 2.223C4.978 12.711 6.427 13 8 13s3.022-.289 4.096-.777C13.125 11.755 14 11.007 14 10v-.839c-.457.432-1.004.751-1.49.972-1.232.56-2.828.867-4.51.867s-3.278-.307-4.51-.867c-.486-.22-1.033-.54-1.49-.972"/>
                 <path d="M2 12.161V13c0 1.007.875 1.755 1.904 2.223C4.978 15.711 6.427 16 8 16s3.022-.289 4.096-.777C13.125 14.755 14 14.007 14 13v-.839c-.457.432-1.004.751-1.49.972-1.232.56-2.828.867-4.51.867s-3.278-.307-4.51-.867c-.486-.22-1.033-.54-1.49-.972"/>
             </svg><span style="margin-left: 3px;word-break: break-all;">${dbnames[db]}</span><ul style="display:none">`
         let schemaname=Object.keys(data[dbnames[db]])
     //    console.log(schemaname);
         for (let sch=0;sch<schemaname.length;sch++){   //get all svhemanames
             htmlstr+=`<li>
                         <span class="m-symbol" onclick="toggleview(this)">
                             <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-plus" viewBox="0 0 16 16">
                             <path d="M8 4a.5.5 0 0 1 .5.5v3h3a.5.5 0 0 1 0 1h-3v3a.5.5 0 0 1-1 0v-3h-3a.5.5 0 0 1 0-1h3v-3A.5.5 0 0 1 8 4"/>
                             </svg>
                         </span>
                           <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="gold" class="bi bi-folder-fill" viewBox="0 0 16 16">
                               <path d="M9.828 3h3.982a2 2 0 0 1 1.992 2.181l-.637 7A2 2 0 0 1 13.174 14H2.825a2 2 0 0 1-1.991-1.819l-.637-7a2 2 0 0 1 .342-1.31L.5 3a2 2 0 0 1 2-2h3.672a2 2 0 0 1 1.414.586l.828.828A2 2 0 0 0 9.828 3m-8.322.12q.322-.119.684-.12h5.396l-.707-.707A1 1 0 0 0 6.172 2H2.5a1 1 0 0 0-1 .981z"/>
                           </svg> 
                          <span style="margin-left: 3px;word-break: break-all;">${schemaname[sch]}</span><ul style="display:none">`
                             
             let VT=Object.keys(data[dbnames[db]][schemaname[sch]]) 
             // console.log(VT);
             for(let tb=0;tb<VT.length;tb++){  //table and views
                 htmlstr+=`<li>
                     <span class="m-symbol" onclick="toggleview(this)">
                         <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-plus" viewBox="0 0 16 16">
                         <path d="M8 4a.5.5 0 0 1 .5.5v3h3a.5.5 0 0 1 0 1h-3v3a.5.5 0 0 1-1 0v-3h-3a.5.5 0 0 1 0-1h3v-3A.5.5 0 0 1 8 4"/>
                         </svg>
                     </span>
                     <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="gold" class="bi bi-folder-fill" viewBox="0 0 16 16">
                                    <path d="M9.828 3h3.982a2 2 0 0 1 1.992 2.181l-.637 7A2 2 0 0 1 13.174 14H2.825a2 2 0 0 1-1.991-1.819l-.637-7a2 2 0 0 1 .342-1.31L.5 3a2 2 0 0 1 2-2h3.672a2 2 0 0 1 1.414.586l.828.828A2 2 0 0 0 9.828 3m-8.322.12q.322-.119.684-.12h5.396l-.707-.707A1 1 0 0 0 6.172 2H2.5a1 1 0 0 0-1 .981z"/>
                               </svg>
                 <span style="margin-left: 3px;word-break: break-all;">${VT[tb]}</span><ul style="display:none;padding:0px">`
                 let table=Object.keys(data[dbnames[db]][schemaname[sch]][VT[tb]]) 
            // console.log(table);
                 for(let tbl=0;tbl<table.length;tbl++){
                     htmlstr+=`<li>
                         <span class="m-symbol" onclick="toggleview(this)">
                             <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-plus" viewBox="0 0 16 16">
                             <path d="M8 4a.5.5 0 0 1 .5.5v3h3a.5.5 0 0 1 0 1h-3v3a.5.5 0 0 1-1 0v-3h-3a.5.5 0 0 1 0-1h3v-3A.5.5 0 0 1 8 4"/>
                             </svg>
                         </span>
                         <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-table" viewBox="0 0 16 16">
                                            <path d="M0 2a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2zm15 2h-4v3h4zm0 4h-4v3h4zm0 4h-4v3h3a1 1 0 0 0 1-1zm-5 3v-3H6v3zm-5 0v-3H1v2a1 1 0 0 0 1 1zm-4-4h4V8H1zm0-4h4V4H1zm5-3v3h4V4zm4 4H6v3h4z"/>
                                       </svg>
                     <span style="margin-left: 3px;word-break: break-all;">${table[tbl]}</span><ul style="display:none">`
                     let exctractcolnames=Object.keys(data[dbnames[db]][schemaname[sch]][VT[tb]][table[tbl]])
                     //console.log(exctractcolnames);
                     for (let dt=0;dt<exctractcolnames.length;dt++){
                         let coltype=data[dbnames[db]][schemaname[sch]][VT[tb]][table[tbl]][exctractcolnames[dt]]
                         // console.log("col datatype",coltype); 
                         // for(let val of cols){
                             var checked='o0o'
                            
                             if(Object.keys(mdata).length > 0){
                                let dec=hasKey(mdata, exctractcolnames[dt])
                                console.log('decide',dec);
                                if(dec){
                                    checked='checked'
                                    let reference=dbnames[db]+">"+schemaname[sch]+">"+VT[tb]+">"+table[tbl]
                                    htmlstr+=`<li class="colnames" data-reference="${reference}">
                                              <input class="form-check-input" type="checkbox" value="${exctractcolnames[dt]}" data-type='`
                                    htmlstr+=`${coltype}' `
                                    htmlstr+=`onchange="getcolumnvalue(this)" ${checked}>
                                              <span style="word-break: break-all; ">${exctractcolnames[dt]}</span></li>`

                                }else{
                                    checked=''
                                    let reference=dbnames[db]+">"+schemaname[sch]+">"+VT[tb]+">"+table[tbl]
                                    htmlstr+=`<li class="colnames" data-reference="${reference}">
                                              <input class="form-check-input" type="checkbox" value="${exctractcolnames[dt]}" data-type='`
                                    htmlstr+=`${coltype}' `
                                    htmlstr+=`onchange="getcolumnvalue(this)" ${checked}>
                                              <span style="word-break: break-all; ">${exctractcolnames[dt]}</span></li>`
                                }
                             }else{
                                checked=''
                                let reference=dbnames[db]+">"+schemaname[sch]+">"+VT[tb]+">"+table[tbl]
                                htmlstr+=`<li class="colnames" data-reference="${reference}">
                                        <input class="form-check-input" type="checkbox" value="${exctractcolnames[dt]}" data-type='`
                                htmlstr+=`${coltype}' `
                                htmlstr+=`onchange="getcolumnvalue(this)" ${checked}>
                                        <span style="word-break: break-all; ">${exctractcolnames[dt]}</span></li>`
                             }
                             
                         // }
                     }
                     htmlstr+=`</ul></li>`    
                 }
                 // console.log("---");
                 htmlstr+=`</ul></li>`
             }   
             htmlstr+=`</ul></li>`
         }
         htmlstr+=`</ul></li>`
            }
        htmlstr+=`</ul>`
        document.getElementsByClassName("db-list")[0].innerHTML = htmlstr;               
        }

function toggleview(element){
    var parentLi = element.closest('li');
    var firstChildUl = parentLi.querySelector(':scope > ul');
    if (firstChildUl) {
        if (firstChildUl.style.display === 'none') {
            firstChildUl.style.display = 'block';
            element.innerHTML=`<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-dash" viewBox="0 0 16 16">
                                    <path d="M4 8a.5.5 0 0 1 .5-.5h7a.5.5 0 0 1 0 1h-7A.5.5 0 0 1 4 8"/>
                                </svg>`
        } else {
            firstChildUl.style.display = 'none';
            element.innerHTML=`<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-plus" viewBox="0 0 16 16">
                                  <path d="M8 4a.5.5 0 0 1 .5.5v3h3a.5.5 0 0 1 0 1h-3v3a.5.5 0 0 1-1 0v-3h-3a.5.5 0 0 1 0-1h3v-3A.5.5 0 0 1 8 4"/>
                               </svg>`
        }
    }
}


function getcolumnvalue(checkbox){
    const parent = checkbox.closest(".colnames");
    const reference = parent.getAttribute("data-reference");
    const columnValue = checkbox.value;
    let datatype=checkbox.getAttribute("data-type")
    // console.log('datatype',datatype);
    const keys = reference.split(">");
    // Initialize the nested structure if it does not exist
    if (!mdata[keys[0]]) mdata[keys[0]] = {};
    if (!mdata[keys[0]][keys[1]]) mdata[keys[0]][keys[1]] = {};
    if (!mdata[keys[0]][keys[1]][keys[2]]) mdata[keys[0]][keys[1]][keys[2]] = {};
    if (!mdata[keys[0]][keys[1]][keys[2]][keys[3]]) mdata[keys[0]][keys[1]][keys[2]][keys[3]] = {};

    // Now safely access the nested structure
    const array =Object.keys(mdata[keys[0]][keys[1]][keys[2]][keys[3]])
    // console.log(array);
    if (array.includes(columnValue)) {
        const index = array.indexOf(columnValue);
        if (index !== -1) {
            array.splice(index, 1); // value is in array then remove
            if (mdata[keys[0]][keys[1]][keys[2]][keys[3]][columnValue] ){
                delete mdata[keys[0]][keys[1]][keys[2]][keys[3]][columnValue];
            }
        }
    } else {
        mdata[keys[0]][keys[1]][keys[2]][keys[3]][columnValue]=datatype; // else add value in array
    }
    // Check if any of the nested arrays are empty and delete them
    if (Object.keys(mdata[keys[0]][keys[1]][keys[2]][keys[3]]).length === 0) {
        delete mdata[keys[0]][keys[1]][keys[2]][keys[3]];
    }
    if (Object.keys(mdata[keys[0]][keys[1]][keys[2]]).length === 0) {
        delete mdata[keys[0]][keys[1]][keys[2]];
    }
    if (Object.keys(mdata[keys[0]][keys[1]]).length === 0) {
        delete mdata[keys[0]][keys[1]];
    }
    if (Object.keys(mdata[keys[0]]).length === 0) {
        delete mdata[keys[0]];
    }
    console.log(mdata);
}


$("#qsubmit").click((e)=>{
    let inp=$('#userinput').val().trim()
   if(inp.length>0){
    $(".showQloader").show()
    $("#show-table-error").html('')
    e.preventDefault()
    const formdata=new FormData()
    formdata.append('qry', inp)
    $.ajax({
        url: '/getquery', 
        method: 'POST',
        data:formdata,
        processData: false, 
        contentType: false, 
        success: function(data) {
            console.log('data',data);
            $(".showQloader").hide()
            if(data.msg =="success"){
              $("#nav-div1").html(data.table)
              $('#nav-div1 table').addClass("table table-hover").css({"width":"96%","margin":"8px auto 8px auto"})
              $("#nav-div1 table thead").addClass("table-secondary")
              $("#nav-div1 table thead tr").css("text-align","left")
              $('#nav-div1 table tbody').css({"height":"400px","overflow-y":"auto"})
              $("#nav-div2").html(data.graph)
              $(".myquery").val(data.query)
            }else{
                let message='Internal server error while creating Query!' 
                let type='danger'
                let div='show-table-error'
                appendAlert(message, type,div)
                $(".myquery").val('')
                $("#nav-div1").html('')
                $("#nav-div2").html('')
            }
        },
        error:function (error){
            console.log(error);
            let message='Internal server error while creating Query!' 
            let type='danger'
            let div='show-table-error'
            appendAlert(message, type,div)
            $(".showQloader").hide()
            $(".myquery").val('')
            $("#nav-div1").html('')
            $("#nav-div2").html('')
        }
    
    })
   }else{
    alert('Please enter the query...')
   }
})

function analyze(element){
  if (Object.keys(mdata).length !== 0){
    console.log('function runned');
    $("#show-table-error").html('')
    $(".showQloader").show()
     const formdata=new FormData()
      formdata.append('schema',JSON.stringify(mdata))
      $.ajax({
          url: '/generatedescription', 
          method: 'POST',
          data:formdata,
          processData: false, 
          contentType: false, 
          success: function(data) {
          console.log('data',(typeof data),data);
          $(".showQloader").hide()
          if (data !='error'){
              
          }else{
              let message='Failed Analyzing database !'
              let type='danger'
              let div='show-table-error'
              appendAlert(message, type,div) 
              
          }
          },
          error:function (error){
             $(".showQloader").hide()
              let message='Failed Analyzing database !'
              let type='danger'
              let div='show-table-error'
              appendAlert(message, type,div) 
          } 
      })
  }else{alert('Select colmuns from tables...')}
   
}

// $("#analyzedb").click((e)=>{
//     console.log('clicked');
//     e.preventDefault()
//     $(".showQloader").show()
//     const formdata=new FormData()
//      formdata.append('schema',mdata)
//      formdata.append('generate_btype','analyze')
//      $.ajax({
//          url: '/generatedescription', 
//          method: 'POST',
//          data:formdata,
//          processData: false, 
//          contentType: false, 
//          success: function(data) {
//          console.log('data',(typeof data),data);
//          $(".showQloader").hide()
//          if (data !='error'){
             
//          }else{
//              let message='Failed Analyzing database !'
//              let type='danger'
//              let div='show-table-error'
//              appendAlert(message, type,div) 
             
//          }
//          },
//          error:function (error){
//             $(".showQloader").hide()
//              let message='Failed Analyzing database !'
//              let type='danger'
//              let div='show-table-error'
//              appendAlert(message, type,div) 
//          } 
//      })
     
//  })

// $("#qsubmit").click((e)=>{
//     $(".showQloader").show()
//     $("#show-query-error").html('')
//     e.preventDefault()
//     $('.getqueryans').html('')
//     const formdata=new FormData()
//     formdata.append('qry', $('#userinput').val())

//     $.ajax({
//         url: '/getquery', 
//         method: 'POST',
//         data:formdata,
//         processData: false, 
//         contentType: false, 
//         success: function(data) {
//             console.log('data',data);
//             if(data!='error' && data !='tryagain'){  
//                 let htmlstr=`<div class="ansbg">ANSWER :</div>
//                 <div class="border  p-3">
//                      <textarea class="watsonoutput form-control" type="text" name="" id="" rows="3" readonly style="resize: none;">${data.Answer}</textarea>
//                 </div>
//                 <div class=" mt-2">
//                     <table class="table table-bordered">
//                         <tbody>
//                             <tr>
//                                 <td class="td1 tdbg">DESCRIPTION </td>
//                                 <td>${data.Description}</td>
//                             </tr>
//                             <!--<tr>
//                                 <td class="td1 tdbg">VARIABLES</td>
//                                 <td>{}</td>
//                             </tr> -->
//                             <tr>
//                                 <td class="td1 tdbg">Tables used</td>
//                                 <td>${data.Tables_used}</td>
//                             </tr>
//                         </tbody>
//                     </table>
//                 </div>
//                 <div class="row mx-0">
//                     <div class="col d-flex justify-content-center p-5 gap-2">
//                         <button class="btn btn-secondary editquery" onclick="editquery()">EDIT <i class="bi bi-pencil-square"></i></button>
//                         <button class="btn btn-primary svbtn" onclick="savequerydata()">SAVE <i class="bi bi-floppy"></i><span class="sloaders" id="sloader3" style="display:none;"></span></button>
//                         <button class="btn btn-danger " onclick="downloadresult()">DOWNLOAD <i class="bi bi-cloud-arrow-down-fill"></i></button>
//                         <button class="btn btn-warning text-white" onclick="showchart()">ANALYZE <i class="bi bi-clipboard2-data"></i></button>
//                         </div>
//                 </div>`
//                 $('.getqueryans').html(htmlstr)
//                 $(".showQloader").hide()
//             }else if(data =='tryagain'){
//                 console.log('tryagain');
//                 let message='Try again rephrasing query !' 
//                 let type='danger'
//                 let div='show-query-error'
//                 appendAlert(message, type,div)
//                 $(".showQloader").hide()
//             }
//             else{
//                 console.log('error 1');
//                 let message='Internal server error while creating Query!' 
//                 let type='danger'
//                 let div='show-query-error'
//                 appendAlert(message, type,div)
//                 $(".showQloader").hide()
//             }
//         },
//         error:function (error){
//             console.log(error);
//             let message='Internal server error while creating Query!' 
//             let type='danger'
//             let div='show-query-error'
//             appendAlert(message, type,div)
//             $(".showQloader").hide()
//         }
    
//     })
// })


function editquery(){
    $(".watsonoutput").prop('readonly', false).css('border', '2px solid blue');
}

columnvalues={}
function showchart(){
    columnvalues={}
    const formdata=new FormData()
    formdata.append('query',$(".watsonoutput").val())
    $.ajax({
        url: '/downloadcsv', 
        method: 'POST',
        data:formdata,
        processData: false, 
        contentType: false, 
        success: function(data) {  
            console.log('gdata',typeof data);  
             let data2=JSON.parse(data)
             $("#myChart").html('')
             $('.addcolumns').html('')
             $("#xaxisinput").val('')
             $("#yaxisinput").val('')
             let colnames=Object.keys(data2[0])
            for(let j=0;j<colnames.length;j++){
                let htmlstring=`<a  class="list-group-item list-group-item-action draggable" draggable="true" ondragstart="drag(event)">${colnames[j]}</a>`
                columnvalues[colnames[j]]=[]  
                $('.addcolumns').append(htmlstring)
            }
            for(let i=0;i<data2.length;i++){
                for(let j=0;j<colnames.length;j++){
                    columnvalues[colnames[j]].push(data2[i][colnames[j]])   
                }
            }
            console.log('columnvalues',columnvalues);
            $('#modalchart').modal('show');
        },
        error:function (error){

        } 
    })
}

let mchart='off'

function generateChart(){
  let chartSelect=$("#chartSelect").val()
  let  xaxisinput =$("#xaxisinput").val()
  let  yaxisinput  = $("#yaxisinput").val()
  let xarray =columnvalues[xaxisinput]
  let yarray =columnvalues[yaxisinput]
  console.log(columnvalues[xaxisinput]);
  console.log(columnvalues[yaxisinput]);
  if(mchart !='off'){
    mchart.destroy()
  }
  if (xaxisinput.value !='' && yaxisinput.value !=''){
    const ctx = document.getElementById('myChart');
    if(chartSelect == 'bar'){
      barchart(ctx,xarray,yarray)
    }else if(chartSelect =='pie'){
      piechart(ctx,xarray,yarray)
    }else{
      linechart(ctx,xarray,yarray)
    }
  }else{

  }
    
  
 
}

function barchart(ctx,xarray,yarray){
    mchart= new Chart(ctx, {
        type: 'bar',
        data: {
          labels: xarray,
          datasets: [{
            label:'barchart',
            data: yarray,
            backgroundColor:colorNames.slice(0,xarray.length),
            borderWidth: 1
          }]
        },
        options: {
          scales: {
            y: {
                beginAtZero: true,
            }
           },
           plugins: {
            customCanvasBackgroundColor: {
              color: 'white',
            }
          }
         },
         plugins: [plugin],
      });
}

function linechart(ctx,xarray,yarray){
     mchart = new Chart(ctx, {
        type: 'line',
        data: {
           labels: xarray,
           datasets: [{
            label:'linechart',
              data: yarray, 
              backgroundColor:colorNames.slice(0,xarray.length),
              borderColor: ['black'],
              borderWidth: 2,
              pointRadius: 5,
           }],
        },
        options: {
           responsive: false,
           plugins: {
            customCanvasBackgroundColor: {
              color: 'white',
            }
          }
        },
        plugins: [plugin],
     });
}

function piechart(ctx,xarray,yarray){
    mchart = new Chart(ctx, {
        type: 'pie',
        data: {
           labels:xarray ,
           datasets: [{
            label:'piechart',
              data:yarray ,
              backgroundColor: colorNames.slice(0,xarray.length),
              hoverOffset: 5
           }],
        },
        options: {
           responsive: false,
           plugins: {
            customCanvasBackgroundColor: {
              color: 'white',
            }
          }
        },
        plugins: [plugin],
     });
}


function downloadcharts(){
    if (mchart !='off'){
        var a = document.createElement('a');
        a.href = mchart.toBase64Image();
        a.download = 'chart.png';
        a.click();
    }
}

function getvalue(data){
  let value=data.value;
  var index = checkedValues.indexOf(value);
  if (index === -1) {
      checkedValues.push(value);
  } else {
      checkedValues.splice(index, 1);
      console.log('Removed:', value);
  }
  if(checkedValues.length === 0){
    $(".submittables").removeAttr('onclick')
    $(".submittables").prop('disabled', true);
  }else{
    $(".submittables").attr('onclick','sendtable()')
    $(".submittables").prop('disabled', false);
  }
    console.log(checkedValues);
}

function selectall(){
   console.log('seletall');
   var checkboxes = document.querySelectorAll('.checkbox');
   checkboxes.forEach(function(checkbox) {
    // Set "checked" property to true
    checkbox.checked = true;

    // Add the checkbox ID to the array if it's not already there
    if (checkedValues.indexOf(checkbox.value) === -1) {
        checkedValues.push(checkbox.value);
    }
  });
  if(checkedValues.length === 0){
    $(".submittables").removeAttr('onclick')
    $(".submittables").prop('disabled', true);
  }else{
    $(".submittables").attr('onclick','sendtable()')
    $(".submittables").prop('disabled', false);
  }
  console.log(checkedValues);
}


// $("#userinput").on('input',()=>{
//    $("#qsubmit").prop('disabled', false);
// })

// $("#userinput").on('blur',()=>{
//     let val=$("#userinput").val()
//     if(val == ''){
//         $("#qsubmit").prop('disabled', true);
//     }else{
//         $("#qsubmit").prop('disabled', false);
//     }
//  })



function sendtable(){
    $("#show-table-error").html('')
    console.log('sendtable',checkedValues);
    const formdata=new FormData()
    formdata.append('tables', checkedValues)
    $("#sloader2").show()
    $.ajax({
        url: '/sendtable', 
        method: 'POST',
        data:formdata,
        processData: false, 
        contentType: false, 
        success: function(data) {
          
          if( data != 'error'){
            console.log(data);
            let message='Table Connection Successful!' 
            let type='info'
            let div='show-table-error'
            appendAlert(message, type,div)
            setTimeout(()=>{
                $("#nav-qry-tab").click()
            },2000)
          }else{
            let message='Table Connection Error !'
            let type='danger'
            let div='show-table-error'
            appendAlert(message, type,div)
          }
          $("#sloader2").hide()
        }
        ,error:function(){
            let message='Table Connection Error !'
            let type='danger'
            let div='show-table-error'
            appendAlert(message, type,div)
            $("#sloader2").hide()
        }
    })
 }


function savequerydata(){
    console.log('save query done1');
    $("#sloader3").show()
    const formdata=new FormData()
    let db_host=$("#hostname").val()
    let db_user=$("#user").val()
    let db_password=$("#password1").val()
    let db_port=$("#portno").val()
    let db_name=$("#database").val()
    formdata.append('connection_string', `mysql+pymysql://${db_user}:${db_password}@${db_host}:${db_port}/${db_name}`)
    formdata.append('query',$("#userinput").val())
    formdata.append('llm_output',$(".watsonoutput").val())
    $.ajax({
        url: '/savequery', 
        method: 'POST',
        data:formdata,
        processData: false, 
        contentType: false, 
        success: function(data) {
           console.log(data);
           $("#sloader3").hide()
           if(data =='success'){
            changeRectColor('green','Query is saved !')
            disconnection_msg()
           }else{
            changeRectColor('red','Query is not saved !')
            disconnection_msg()
           }
        },
        error:function (error){
            $("#sloader3").hide()
            changeRectColor('red','Query is not saved !')
            disconnection_msg()
        } 
    })          
}

// $("#analyzedb").click(()=>{
//    $("#sloader3").show() 
//    const formdata=new FormData()
//     formdata.append('generate_btype','analyze')
//     $.ajax({
//         url: '/generatedescription', 
//         method: 'POST',
//         data:formdata,
//         processData: false, 
//         contentType: false, 
//         success: function(data) {
//         console.log('data',(typeof data),data);
//         if (data !='error'){
//             $.ajax({
//             url: '/generateexample', 
//             method: 'POST',
//             data:formdata,
//             processData: false, 
//             contentType: false, 
//             success: function(data2) {
//             console.log('data2',(typeof data2),data2);
//              if (data2 !='error'){
//                 $("#savedb-description").modal('show')
//                 $("#sloader3").hide()
//                 console.log(typeof(data))
//                 if(typeof(data) == 'string'){
//                     $("#dexcjson").html(`<pre>${syntaxHighlight(JSON.stringify(JSON.parse(data), undefined, 2))}</pre>`);
//                     $("#exmpljson").html(`<pre>${syntaxHighlight(JSON.stringify(JSON.parse(data2), undefined, 2))}</pre>`);
//                 }else{
//                     $("#dexcjson").html(`<pre>${syntaxHighlight(JSON.stringify(data, undefined, 2))}</pre>`);
//                     $("#exmpljson").html(`<pre>${syntaxHighlight(JSON.stringify(data2, undefined, 2))}</pre>`);
//                 }
//              }else{
//                 let message='Failed Analyzing database !'
//                 let type='danger'
//                 let div='append-db-error'
//                 appendAlert(message, type,div) 
//                 $("#sloader3").hide()
//              }
//             },
//             error:function (error){
//                 let message='Failed Analyzing database !'
//                 let type='danger'
//                 let div='append-db-error'
//                 appendAlert(message, type,div) 
//                 $("#sloader3").hide()
//             }
//             })
//         }else{
//             let message='Failed Analyzing database !'
//             let type='danger'
//             let div='append-db-error'
//             appendAlert(message, type,div) 
//             $("#sloader3").hide()
//         }
//         },
//         error:function (error){
//             let message='Failed Analyzing database !'
//             let type='danger'
//             let div='append-db-error'
//             appendAlert(message, type,div) 
//             $("#sloader3").hide()
//         } 
//     })
    
// })


$("#editdesc").click(()=>{
    $("#dexcjson").attr('contenteditable', true);
})
$("#editdexpl").click(()=>{
    $("#exmpljson").attr('contenteditable', true);
})

$("#savedesc").click(()=>{
    $("#dexcjson").attr('contenteditable', false);
    console.log($("#dexcjson pre").text());
    const formdata=new FormData()
    formdata.append('desc',$("#dexcjson pre").text())
    $.ajax({
        url: '/savedescription', 
        method: 'POST',
        data:formdata,
        processData: false, 
        contentType: false, 
        success: function(data) {
           console.log(data);
           if(data == 'success'){
            changeRectColor('green','Description is saved !')
            disconnection_msg()
           }else{
            changeRectColor('red','Error while saving description !')
            disconnection_msg()
           }
           
        },
        error:function (error){
            changeRectColor('red','Error while saving description !')
            disconnection_msg()
        }
    })
})


$("#saveexpl").click(()=>{
   $("#exmpljson").attr('contenteditable', false);
   console.log($("#exmpljson pre").text());
    const formdata=new FormData()
    formdata.append('exmpl',$("#exmpljson pre").text())
    $.ajax({
        url: '/savedexample', 
        method: 'POST',
        data:formdata,
        processData: false, 
        contentType: false, 
        success: function(data) {
           console.log(data);
           if(data =='success'){
            changeRectColor('green','Example is saved !')
            disconnection_msg()
           }else{
            changeRectColor('red','Error while saving example !')
            disconnection_msg()
           }
        },
        error:function (error){
            changeRectColor('red','Error while saving example !')
            disconnection_msg()
        }
    })
})

$('#rdesc').click(()=>{
    $("#sloader4").show()
    const formdata=new FormData()
    formdata.append('hostname', $("#hostname").val())
    formdata.append('user',$("#user").val())
    formdata.append('password',$("#password1").val())
    formdata.append('portno',$("#portno").val())
    formdata.append('database',$("#database").val())
    formdata.append('generate_btype','rdescription')
    $.ajax({
        url: '/generatedescription', 
        method: 'POST',
        data:formdata,
        processData: false, 
        contentType: false, 
        success: function(data) {
           if(data !='error'){
            $("#sloader4").hide()
            $("#dexcjson").html('')
            setTimeout(()=>{
                $("#dexcjson").html(`<pre>${syntaxHighlight(JSON.stringify(data, undefined, 2))}</pre>`);            
            },3000)
           }else{
            $("#sloader4").hide()
           }
        },
        error:function (error){
            $("#sloader4").hide()
        }

    })
})
$('#rexml').click(()=>{
    $("#sloader5").show()
    console.log('btn clicked');
    const formdata=new FormData()
    formdata.append('hostname', $("#hostname").val())
    formdata.append('user',$("#user").val())
    formdata.append('password',$("#password1").val())
    formdata.append('portno',$("#portno").val())
    formdata.append('database',$("#database").val())
    formdata.append('generate_btype','rexample')
    $.ajax({
        url: '/generateexample', 
        method: 'POST',
        data:formdata,
        processData: false, 
        contentType: false, 
        success: function(data2) {
            print('data2 re',data2)
         if (data2 !='error'){
            $("#sloader5").hide()
            $("#exmpljson").html('')
            setTimeout(()=>{
                $("#exmpljson").html(`<pre>${syntaxHighlight(JSON.stringify(data2, undefined, 2))}</pre>`);
            },2000)
         }else{
            $("#sloader4").hide() 
         } 
        },
        error:function (error){
            $("#sloader4").hide()
        }
        })
})

function downloadresult(){
    const formdata=new FormData()
    formdata.append('query',$(".watsonoutput").val())
    $.ajax({
        url: '/downloadcsv', 
        method: 'POST',
        data:formdata,
        processData: false, 
        contentType: false, 
        success: function(data) {
         console.log(data);  
        downloadCsvFromJson(JSON.parse(data), 'sample.csv')
        },
        error:function (error){
        } 
    })
}
function downloadCsvFromJson(jsonData, fileName) {
    // Convert JSON to CSV String
    function jsonToCsv(jsonData) {
      const header = Object.keys(jsonData[0]).join(',');
      const rows = jsonData.map(obj => Object.values(obj).join(','));
      return `${header}\n${rows.join('\n')}`;
    }
  
    // Create Blob
    const csvData = jsonToCsv(jsonData);
    const blob = new Blob([csvData], { type: 'text/csv' });
  
    // Create download link
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = fileName || 'data.csv';
    document.body.appendChild(link);
    // Trigger download
    link.click();
  
    // Clean up
    document.body.removeChild(link);
    URL.revokeObjectURL(link.href);
  }

$(".logout").click(()=>{
    var cookies = document.cookie.split(";");
    for (var i = 0; i < cookies.length; i++) {
        var cookie = cookies[i];
        var eqPos = cookie.indexOf("=");
        var name = eqPos > -1 ? cookie.substr(0, eqPos) : cookie;
        document.cookie = name + "=;expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;";
    }
})

function disconnect(){
    $("#append-db-error").html('')
    $.ajax({
        url: '/disconnect', 
        method: 'GET',
        processData: false, 
        contentType: false, 
        success: function(data) {
         if(data!='error'){
            console.log('green');
            changeRectColor('green','Database connection Disconnected !')
            disconnection_msg()
            $(".latestconn").hide()
            $(".latdb").html('')
            $(".latdbusername").text('')
            $(".latdbname").text('')
            $('.getqueryans').html('')
            $('#userinput').val('')
            $('#db-form')[0].reset();
            $(".myquery").val('')
            $("#nav-div1").html('')
            $("#nav-div2").html('')
            document.getElementsByClassName('db-list')[0].innerHTML=''
            mdata={}
            // $("#showmf").hide()
         }else{
            changeRectColor('red','Error while disconnecting database !')
            disconnection_msg()
         }
        },
        error:function (error){
            changeRectColor('red','Error while disconnecting database !')
            disconnection_msg()
        } 
    })
}

function disconnection_msg(){
    const toastLiveExample = document.getElementById('liveToast')
    if (toastLiveExample) {
    const toastBootstrap = bootstrap.Toast.getOrCreateInstance(toastLiveExample)
        toastBootstrap.show()
    }
}

function changeRectColor(newColor,msg) {
    $('#toastRect rect').attr('fill', newColor);
    $('.toast-body span').text(msg);
}