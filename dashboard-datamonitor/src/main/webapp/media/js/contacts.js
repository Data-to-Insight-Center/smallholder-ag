//var editor; // use a global for the submit and return data rendering in the contact
var otable;
var dataTab;
var textit_prefix = "./api/zambia";
$(document).ready(function() {
	$.fn.dataTable.ext.errMode = 'none';
	
    editor = new $.fn.dataTable.Editor( {
        table: "#contact",
        fields: [ {
                label: "Contact UUID:",
                name: "uuid",
				type: "readonly"
            }, {
                label: "Contact Name:",
                name: "name",
				type: "readonly"
            }, {
                label: "Phone No:",
                name: "phone",
				type: "readonly"
            }, {
                label: "Country:",
                name: "country",
				type: "readonly"
            }, {
                label: "Date Enrolled:",
                name: "date_enrolled",
				type: 'datetime',
                format: 'YYYY-MM-DD HH:mm:ss',
                fieldInfo: 'EX: 03-10-2016 2.30 PM'
            }, {
                label: "Network:",
                name: "network",
				fieldInfo: "Ex: Network-1",
				type: "readonly"
            }, {
                label: "Enrollment Status:",
                name: "enroll_status",
				fieldInfo: "Ex: Enrolled, Disenrolled"
            }, {
                label: "Longitude:",
                name: "longitude",
				fieldInfo: "Ex: 27.8493° E"
            }, {
                label: "Latitude:",
                name: "latitude",
				fieldInfo: "Ex: 13.1339° S"
            }, {
                label: "HH ID:",
                name: "hh_id",
				fieldInfo: "Ex: hh1"
            }, {
                label: "Village:",
                name: "village",
				fieldInfo: "Ex: Zambian Village"
            }, {
                label: "Camp:",
                name: "camp",
				fieldInfo: "Ex: Lusaka"
            }
        ]
    } );
	
	
	$('#contact').on( 'click', 'tbody td:not(:first-child)', function (e) {
        editor.inline( this, {
            buttons: { label: '&gt;', fn: function () { 
			
			var table = $('#contact').DataTable();
			
			$('div .DTE_Form_Buttons').click(function () {			
				
				var date_enrolled = $("#DTE_Field_date_enrolled").val();		
				var latitude = $("#DTE_Field_latitude").val();
				//var network = $("#DTE_Field_network").val();
				var longitude = $("#DTE_Field_longitude").val();
				var hh_id = $("#DTE_Field_hh_id").val();
				var village = $("#DTE_Field_village").val();
				var camp = $("#DTE_Field_camp").val();
				var enroll_status = $("#DTE_Field_enroll_status").val();
				
				var uuid = $(this).closest('tr').find('td:eq(1)').text();
				
				
				if(date_enrolled==undefined){
					date_enrolled = $(this).closest('tr').find('td:eq(4)').text();
					
					if(date_enrolled==""){
					contact_date_enrolled = "";
					contact_time_enrolled = "";
					}else{
					contact_date_enrolled_sa = new Date(date_enrolled).toISOString();
					contact_date_enrolled = contact_date_enrolled_sa.split("T")[0];
					contact_time_enrolled = contact_date_enrolled_sa.split("T")[1];
					}
					
				}else{
					contact_date_enrolled_sa = new Date(date_enrolled).toISOString();
					contact_date_enrolled = contact_date_enrolled_sa.split("T")[0];
					contact_time_enrolled = contact_date_enrolled_sa.split("T")[1];
				
				}if(enroll_status==undefined){
					enroll_status = $(this).closest('tr').find('td:eq(5)').text();
				}if(longitude==undefined){
					longitude = $(this).closest('tr').find('td:eq(6)').text();
				}if(latitude==undefined){
					latitude = $(this).closest('tr').find('td:eq(7)').text();
				}if(hh_id==undefined){
					hh_id = $(this).closest('tr').find('td:eq(8)').text();
				}if(village==undefined){
					village = $(this).closest('tr').find('td:eq(9)').text();
				}if(camp==undefined){
					camp = $(this).closest('tr').find('td:eq(10)').text();
				}
				
				var dataArray = {};
				dataArray['uuid'] = uuid;
				
				if (latitude != ""){
					dataArray['latitude'] = latitude;
				}if (longitude != ""){
					dataArray['longitude'] = longitude;
				}if (enroll_status != ""){
					dataArray['enroll_status'] = enroll_status;
				}if (contact_date_enrolled != ""){
					dataArray['date_enrolled'] = contact_date_enrolled;
				}if (contact_time_enrolled != ""){
					dataArray['contact_time_enrolled'] = contact_time_enrolled;
				}if (hh_id != ""){
					dataArray['hh_id'] = hh_id;
				}if (village != ""){
					dataArray['village'] = village;
				}if (camp != ""){
					dataArray['camp'] = camp;
				}
				
				$.ajax({
					type: "POST",
					//in this method insert the data in your database
					url: textit_prefix + "/contacts",
					contentType: "application/json; charset=utf-8",
					data: JSON.stringify(dataArray),
					
					dataType: "json",
					success: AjaxUpdateDataSucceeded,
					error: AjaxUpdateDataFailed
				});
			});
			
			
			
			} }
        } );
    } );
 
    $('#contact').DataTable( {
        dom: "Bfrtip",
        ajax: {
			url: textit_prefix + "/contacts",
            dataSrc : ""
		},
        columns: [
			{
                data: null,
                defaultContent: '',
                className: 'select-checkbox',
                orderable: false
            },
			{ data: "uuid", "sClass":"uuid_hide"},
			{ data: "name" },
			{ data: "phone" },
			{ data: "country", "visible": false },
			{ data: "date_enrolled",
			 "render": function (data) {
				if (data != undefined){
					var mom_date = moment(data);
					var new_date = mom_date.tz('Africa/Johannesburg').format('MMMM Do YYYY');
        			return (new_date);
					}
				}
			},
			{ data: "network", "visible": false },
			{ data: "enroll_status" },
			{ data: "longitude" },
			{ data: "latitude" },
			{ data: "hh_id" },
			{ data: "village"},
			{ data: "camp"}
        ],
		
        select: {
            style:    'os',
            selector: 'td:first-child'
        },
        buttons: [
            { extend: "edit", editor: editor }
        ]
    } );

} );

function AjaxUpdateDataSucceeded(result) {
    if (result != "[]") {
		$('#success').html(""); //Clear error span
    	$('#success').append( "<span class='alert alert-success'><strong>Success! </strong>" + ' ' +" Metadata field successfully updated" + "<br></span>");
		setTimeout(function(){
		   window.location.reload();
		}, 2000);
		
    }
}

function AjaxUpdateDataFailed(result) {
	//alert(result.responseText);
    //alert(result.status + ' ' + result.statusText);
	$('#failure').html(""); //Clear error span
    $('#failure').append("<span class='alert alert-danger'><strong>Failure! </strong>" + ' ' + result.responseText + "<br></span>");
	setTimeout(function(){
		   window.location.reload();
	}, 2000);
}

