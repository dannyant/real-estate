from pyspark.sql import SparkSession
import re

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType

text = """

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">




<html>
<HEAD>

<META http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<META name="GENERATOR" content="IBM Software Development Platform">
<META http-equiv="Content-Style-Type" content="text/css">


<!-- Contains script to:  Resize window in Netscape; Identify browser version; Run search; Run department list-->
<SCRIPT LANGUAGE="JavaScript" TYPE="text/javascript" src="/ssi/countypage.js"></SCRIPT>

<!-- Standard stylesheet link for all county pages;-->
<link type="text/css" rel="STYLESHEET" href="/ssi/onlineServices.css">
<!-- Stylesheet for property tax html pages, also used by PropTaxPublic application;-->
<link type="text/css" rel="STYLESHEET" href="/propertytax/proptaxpublic.css">
<!-- PropTaxPublic application specific stylesheet;-->
<LINK href="/ptax_pub_app/theme/master.css?v=3" rel="stylesheet" type="text/css">


<TITLE>Search Secured Supplemental Property Taxes</TITLE>
</HEAD>

<body bgcolor="#FFFFFF" TOPMARGIN=0 LEFTMARGIN=0 MARGINWIDTH=0 MARGINHEIGHT=0 onLoad="window.focus()">

	<table width="100%" border="0" cellspacing="0" cellpadding="0">
		<tr>
  			<td colspan="2">
<html>		
	<head>
	</head>
	<body>
	<TABLE height=26 cellSpacing=0 cellPadding=0 width="100%" border=0>
		<TR>
			<td>
			<!-- ========================================================================================= -->
			<!-- Alameda County Header"-->
			
				<header id="newacgovheader" aria-labelledby="newacgovheader" style="width: 100%;background-color: #dddddd;background-image:none;text-align: left;min-width: 970px;border-bottom: 1px solid #333;"><ul id="acgovheadermenu" style="display:none;"><li class="acquick"></li></ul>
	
	<div style="float: right;padding:0px;margin:0px;height:52px;width:411px;text-align:right;"><a href="#mainsite" style="font-size: 70%;padding: 10px;">Skip County Header</a><br>
		
		<!-- <div id="google_translate_element"></div>

<script type="text/javascript">
function googleTranslateElementInit() {
  new google.translate.TranslateElement({pageLanguage: 'en', layout: google.translate.TranslateElement.InlineLayout.HORIZONTAL}, 'google_translate_element');
}
</script>

<script type="text/javascript" src="//translate.google.com/translate_a/element.js?cb=googleTranslateElementInit"></script> -->
	
	</div>
	
    <!-- <div style="float: right;padding:0px;margin:0px;height:52px;width:411px;text-align:right;"><a href="#mainsite" style="font-size: 70%;padding: 10px;">Skip County Header</a></div> -->
	<div><a href="http://www.acgov.org/index.htm"><img style="margin: 3px;" src="/img/new-acgov-logo-standard-header.png" alt="Alameda County, CA, acgov.org" width="180" height="44" /></a>
	<!--<span style="display:block;text-align: center;margin: 10px; color:red; padding: 10px 0px;font-size: 18px;background-color: #ffffcc;">Alameda County websites may experience intermittent down times this Sunday, July 26, from 6:00 a.m. to 12:00 p.m. due to maintenance.</span></div>-->
  	
	<div style="clear:both;padding:0px;margin:0px;"></div>
</header>	
<div id="mainsite" aria-labelledby="mainsite" role="main" style="clear: both;padding-top: 0px;">
			
			<!-- ========================================================================================= -->
			</td>
		</TR>
	</TABLE>
	
	</body>
</html></td>
		</tr>
		
	</table>



<div id="taxbodysection">


	
		<div id="rl"><a class="ttc" href="/treasurer/">Treasurer-Tax Collector</a> | <a class="bl" href="/business/buslic.htm">Business License</a></div>
	
	<div id="taxbanner">
				<img class="topleft" src="/propertytax/images/title_top_left.gif" alt="" />
				<p class="maintitle">Property Taxes</p>
				<img class="topright" src="/propertytax/images/title_top_right.gif" alt="" />
	</div>
	
		<div id="taxsidebar">
			
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">



<html>
<HEAD>

<META http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<META name="GENERATOR" content="IBM Software Development Platform">
<META http-equiv="Content-Style-Type" content="text/css">
<style type="text/css">
#taxsidebar ul li a.lookup {
	background-color:#ffffff;
	color: #003366;
	font-weight:bold;
}
#taxsidebar ul li a.lookup:hover {
	padding-left: 5px;
}
</style>
</HEAD>




	
			<!-- ========================================================================================= -->
			<!-- Property Tax Public Sidebar-->
			<!-- <img class="topleft" src="/propertytax/images/sidebar_top_left.gif" alt="" /> -->

<ul>
    <li><div id="sidebar-countdown">
            <!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">

<html>
<head>
  <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.6.0/jquery.min.js"></script>
  <script type="text/javascript" src="/propertytax/js/countdown.js"></script>
<style>
  
 .ct-list {
   display: inline-block;
   font-size: 0.78em;
   list-style-type: none;
   padding: 1em;
   text-transform: uppercase;
 }
 
 .ct-list span {
   display: block;
   font-size: 1rem;
   font-weight: bold;
 }
 /* #countdown h2 {
    text-align: center;
  } */
 #countdown ul{
   margin:0;
 }
 </style>
  
</head>

<body>
    <!-- countdown clock starts here -->

    <div id="countdown">
        <p class="heading-text clock-red">
          <!-- <span id="headline"></span> -->
          
          <span id="forecastLabel"></span>
          <span id="forecastDate"></span>
        </p>
      
	 <ul>
	   <li class="ct-list"><span id="days"></span>days</li>
	   <li class="ct-list"><span id="hours"></span>Hours</li>
	   <li class="ct-list"><span id="minutes"></span>Minutes</li>
	   <!-- <li class="ct-list"><span id="seconds"></span>Seconds</li> -->
	 </ul>
   <p class="small-text"><span id="latePayLabel"></span></p>
   <span style="display:none;" id="returnCode"></span>
   
   <!-- <span style="display:none;" id="securedBillYear"></span>
   <span style="display:none;" id="priorRollYear"></span> -->
 
 
 
<!-- countdown clock ends here -->
  </div>

  <!-- API js -->
  <!-- <script type="text/javascript">
    var data;
   
    jQuery.ajax({
     url: "/ptax_pub_app/PropTaxWebService.do?getPropertyTaxInfo=true", 
     success: function (response) {
     
       data = response;
       
       document.getElementById("latePayLabel").innerHTML = data.latePayLabel;
       document.getElementById("forecastLabel").innerHTML = data.forecastLabel;
       document.getElementById("forecastDate").innerHTML = data.forecastDate;
       document.getElementById("securedBillYear").innerHTML = data.securedBillYear;
	     document.getElementById("priorRollYear").innerHTML = data.priorRollYear;
     },
     error: function (response) {
     console.log(response);
     },
   }); 
    </script> -->
</body>
</html>
 
        </div>
    </li>
    <li><a class="email" href="/propertytax/email.htm">Payment Reminders</a></li>
    <li><a class="lookup" href="/propertytax/index.htm">Pay / Look Up Property Taxes Online</a></li>
    <li><a class="methods" href="/propertytax/methods.htm">Other Ways to Pay</a></li>
    <li><a class="appforms" href="/propertytax/forms.htm">Application Forms</a></li>
    <li><a class="monthlypayments" href="/propertytax/monthlypayments.htm">Secured Monthly Payments</a></li>

</ul>
<!-- <img class="bottomleft" src="/propertytax/images/sidebar_bottom_left.gif" alt="" /> --> 
			<!-- ========================================================================================= -->
		


<!-- <BODY>
<table>
<tr>
  <td>
    <h2>Menu</h2>
  </td>
</tr>
<tr>
  <td width="20%" valign="top">
    <a href="">Pay Online</a>
  </td>
</tr>
<tr>
  <td width="20%" valign="top">
    <a href="">Search Taxes</a>
  </td>
</tr>
<tr>
  <td width="20%" valign="top">
    <a href="">Search History</a>
  </td>
</tr>
</table>
</BODY> -->
</html>

		</div>
	
	
		<div id="taxmain">
		<div id="taxcontent">
		<!-- <div id="taxcontent"> -->
			<img class="topright" src="/propertytax/images/main_top_right.gif" alt="" />
			<img class="bottomleft" src="/propertytax/images/main_bottom_left.gif" alt="" />
			<img class="bottomright" src="/propertytax/images/main_bottom_right.gif" alt="" />
			
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">




<html>
<HEAD>

<META http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<META name="GENERATOR" content="IBM Software Development Platform">
<META http-equiv="Content-Style-Type" content="text/css">
<TITLE></TITLE>


<SCRIPT LANGUAGE="JavaScript">
<!-- Begin

	function clearSearchForm()  {
		document.getElementById("displayApn").value='';
		document.getElementById("situsStreetNumber").value='';
		document.getElementById("situsStreetName").value='';
		document.getElementById("situsStreetSuffix").value='';
		document.getElementById("situsUnitNumber").value='';
		document.getElementById("situsCity").value='';
		document.getElementById("displayApn").focus();
	}
	
//-->
</script>

</HEAD>

<BODY>



<!-- ********************************************************************************************** -->
<!-- *                                        BEAN DEFINITION                                     * -->
<!-- ********************************************************************************************** -->

<!-- Get size of current bills array and set boolean -->

	




	


<!-- Get size of prior (history) bills array and set boolean -->

	




	


<!-- Is SDSBill is present and set boolean -->


	




<!-- ********************************************************************************************** -->
<!-- *                                     SCREEN TITLE AND ERROR MESSAGES                        * -->
<!-- ********************************************************************************************** -->

<form name="RealSearchForm" method="post" action="/ptax_pub_app/RealSearch.do">


<table width="100%" align="center" cellspacing="0" border="0">
  	<tr>
  	  	<td align="right" colspan="3">&nbsp;</td>
  	</tr>
	<tr class="pplscreentitle">
  		<td align="left" width="5%">&nbsp;</td>	
  	  	<td align="center" width="90%">Search Secured, Supplemental and Prior Year Delinquent Property Taxes</td>
  	  	<td align="left" width="5%">&nbsp;</td>
  	</tr>
  	<tr class="pplscreensubtitle">
  	  	<td align="left">&nbsp;</td>	
  	  	<td align="center">Secured tax bills are payable online from 10/5/2022 to 6/30/2023.</td>
  	  	<td align="left">&nbsp;</td>
  	</tr>
  	<tr class="pplscreensubtitle">
  	  	<td align="left">&nbsp;</td>	
  	  	<td align="center">Most supplemental tax bills are payable online to 7/1/2024.</td>
  	  	<td align="left">&nbsp;</td>
  	</tr>
  	<tr class="pplscreensubtitle">
  	  	<td align="left">&nbsp;</td>	
  	  	<td align="center">Prior Year Delinquent tax payments are payable online to 7/1/2024.</td>
  	  	<td align="left">&nbsp;</td>
  	</tr>
</table>

			<!-- Show Struts Error Messages if they Exist -->
<table border="0" cellpadding="0" align="center" cellspacing="0" width="95%">
	<tr align="center"><td class="pplerrortext"><br></td></tr>
</table>


             <!-- Show "No Bills Found" Message if no Results Returned -->
 
	




<!-- ********************************************************************************************** -->
<!-- *                    BEGINNING OF ACCOUNT ENTRY FORM - PARCEL NUMBER ENTRY BOX               * -->
<!-- ********************************************************************************************** -->




<!-- ********************************************************************************************** -->
<!-- *                    END OF ACCOUNT ENTRY FORM - BEGINNING OF RESULTS                        * -->
<!-- ********************************************************************************************** -->


<!--       Display the parcel and property address if EITHER current or prior bills found       -->

	<table id="pplresulttable3" width="90%" align="center">
  		<tr>
    		<td>
      			<table id="pplresultcontent3" width="100%" align="center" cellspacing="0">
        			<tr class="pplresultheader">
          				<td align="left" width="25%">Property Summary</td>
          				<td align="center" width="25%">&nbsp;</td>
          				<td align="center" width="25%">&nbsp;</td>
          				<td align="center" width="25%">
          					<input type="submit" name="newBillSearch" value="New Search" class="btcommon">
       					</td>
        			</tr>
        			<tr>
          				<td align="right" class="pplresultlabel" width="25%">APN:&nbsp;</td>
          				<td align="left" class="pplresultdata">&nbsp;46-5448-22</td>
          				<td align="center" class="pplresultdata">&nbsp;</td>
          				<td align="center" class="pplresultdata">&nbsp;</td>
        			</tr>
        			<tr>
          				<td align="right" class="pplresultlabel">Property Address:&nbsp;</td>
          				<td colspan="2" class="pplresultdata" align="left">&nbsp;9714 CHERRY ST, OAKLAND 94603-2647</td>
          				<input type="hidden" name="formattedFullSitusAddress" value="9714 CHERRY ST, OAKLAND 94603-2647">
          				<td align="center" class="pplresultdata">&nbsp;</td>
        			</tr>
       			</table>
    		</td>    
  		</tr>
  	</table>

 

	<table width="100%" align="center" cellspacing="0" border="0">
		
		<tr>
  			<td>&nbsp;</td>
  		</tr>
		<tr>
  	  		<td align="center" class="pplstandardlinks">
  	  			
					
	  	  			<a href="https://propinfo.acgov.org/?PRINT_PARCEL=46-5448-22">Property Assessment Information</a>
  	  			
  	  		</td>
  		</tr>
  		<tr>
  			<td>&nbsp;</td>
  		</tr>
  	</table>



<!-- ********************************************************************************************** -->
<!-- *                             DISPLAY CURRENT BILL RESULTS                                   * -->
<!-- ********************************************************************************************** -->




	<table id="pplresulttable4" width="90%" align="center">
  		<tr>
    		<td>
      			<table width="100%" align="center" cellspacing="0">
        			<tr class="pplresultheader">
          				<td align="left" width="40%">Current Year Tax Information</td>
          				<td align="center" width="10%">&nbsp;</td>
          				<td align="center" width="25%">&nbsp;</td>
          				<td align="center" width="25%">&nbsp;</td>
        			</tr>
       			</table>
    		</td>    
  		</tr>
  		<tr>
    		<td>
      			<table id="pplresultcontent4" width="100%" cellspacing="0" border="0">
      				
					
					<tr class="pplresultlabel">
          				<td align="left" width="3%">&nbsp;</td>
          				<td align="left" width="24%">Tax Type</td>
          				<td align="left" width="15%">Bill Year</td>
          				<td align="left" width="15%">Tracer</td>
          				<td align="left" width="15%">Total Amount</td>
          				<td align="center" width="25%">Options</td>
          				<td align="left" width="3%">&nbsp;</td>
        			</tr>
        			<tr class="pplresultlabel">
          				<td align="left" width="3%">&nbsp;</td>
          				<td align="center" width="24%">Installment</td>
          				<td align="right" width="15%">Due Date&nbsp;&nbsp;</td>
          				<td align="left" width="15%">&nbsp;</td>
          				<td align="right" width="15%">Installment Amount</td>
          				<td align="center" width="25%">Status/Status Date</td>
          				<td align="left" width="3%" >&nbsp;</td>
        			</tr>
        			
        			
         				
         				<tr class=pplevenrow>
       	  					<td align="left" width="3%">&nbsp;</td>
          					<td align="left" width="24%">Secured</td>
          					<td align="left" width="15%">2022-2023</td>
          					<td align="left" width="15%">10198600</td>
          					<td align="left" width="15%">$10,062.76</td>
		  					<td align="left" width="25%">&nbsp;
 		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubCurrRollYear=2022&amp;pubBillId=10198600&amp;pubBillType=SEC" tabindex="3" class="pplviewbill">View Bill</a>
    		    	  			
								&nbsp;&nbsp;&nbsp;&nbsp;
								
							</td>
          					<td align="left" width="3%">&nbsp;</td>
        				</tr>
        				<tr class=pplevenrow>
       	  					<td align="left" width="3%">&nbsp;</td>
          					<td align="center" width="24%">1st Installment</td>
          					<td align="right" width="15%">12/10/2022&nbsp;&nbsp;</td>
          					<td align="left" width="15%">&nbsp;</td>
          					<td align="right" width="15%">$5,026.38</td>
		  					<td align="right" width="28%" colspan="2">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pplevenrow>
	      					<td align="left" width="3%">&nbsp;</td>
       	  					<td align="center" width="24%">2nd Installment</td>
          					<td align="right" width="15%">04/10/2023&nbsp;&nbsp;</td>
          					<td align="left" width="15%">&nbsp;</td>
          					<td align="right" width="15%">$5,036.38</td>
		  					<td align="right" width="28%" colspan="2">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
	    		</table>
	      	</td>
      	</tr>
    </table>
    <br>&nbsp;
  

<!-- ********************************************************************************************** -->
<!-- *                             DISPLAY PRIOR YEAR DELINQUENT TAX INFORMATION                  * -->
<!-- ********************************************************************************************** -->



<table id="pplresulttable4" width="90%" align="center">
  		<tr>
    		<td>
      			<table width="100%" align="center" cellspacing="0">
        			<tr class="pplresultheadererror">
          				<td align="left" width="60%">Prior Year Delinquent Tax Information</td>
          				<td align="center" width="20%">&nbsp;</td>
          				<td align="center" width="20%">&nbsp;</td>
        			</tr>
       			</table>
    		</td>    
  		</tr>
  		<tr>
    		<td>
      			<table id="pplresultcontent4" width="100%" cellspacing="0" border="0">
					<tr class="pplresultlabel">
          				<td align="left" width="3%">&nbsp;</td>
          				<td align="left" width="24%">Parcel Number</td>
          				<td align="left" width="20%">Default Number</td>
          				<td align="left" width="20%">Amount Due</td>
          				<td align="center" width="30%">Status/Status Date</td>
          				<td align="left" width="3%">&nbsp;</td>
        			</tr>
        			<tr class="pplevenrow">
        				 
        				 
        				
        	 			
       	  				<td align="left" width="3%">&nbsp;</td>
          				<td align="left" width="24%">46-5448-22</td>
          				<td align="left" width="20%">797980</td>
          				<td align="left" width="20%">$38,770.50</td>
          				<td align="center" width="30%">
          					
          						<a href="/ptax_pub_app/BillPay.do?showPymtOptions=true&amp;pubBillId=46-5448-22&amp;pubDefaultYear=2021&amp;pubBillType=SDS" tabindex="5" class="pplpaybill">Pay Bill</a>
          					 
          				</td>
          				<td align="left" width="3%">&nbsp;</td>
          			</tr>
          		</table>
          	</td>
          </tr>
         </table>
            
         
         



<!-- ********************************************************************************************** -->
<!-- *                                      DISPLAY TAX HISTORY                                   * -->
<!-- ********************************************************************************************** -->


	
		<br>&nbsp;
			<table border="0" cellpadding="0" align="center" cellspacing="0" width="95%">
				<tr align="center"><td class="pplerrortext">Prior year unpaid amounts below do not include additional penalties, interests and fees. Call (510)272-6800 if amount due is not stated above.</td></tr>
			</table>
		<br>&nbsp;
	
        
	<table id="pplresulttable5" width="90%" align="center">
  		<tr>
    		<td>
      			<table width="100%" align="center" cellspacing="0">
        			<tr class="pplresultheader">
          				<td align="left" width="40%">Prior Year Tax History</td>
          				<td align="center" width="10%">&nbsp;</td>
          				<td align="center" width="25%">&nbsp;</td>
          				<td align="center" width="25%">&nbsp;</td>
        			</tr>
       			</table>
    		</td>    
  		</tr>
  		<tr>
    		<td>
      			<table id="pplresultcontent5" width="100%" cellspacing="0" border="0">
					
					<tr class="pplresultlabel">
          				<td align="left" width="3%">&nbsp;</td>
          				<td align="left" width="24%">Tax Type</td>
          				<td align="left" width="15%">Bill Year</td>
          				<td align="left" width="15%">Tracer</td>
          				<td align="left" width="15%">Total Amount</td>
          				<td align="center" width="25%">Options</td>
          				<td align="left" width="3%">&nbsp;</td>
        			</tr>
        			<tr class="pplresultlabel">
          				<td align="left">&nbsp;</td>
          				<td align="center">Installment</td>
          				<td align="right">Due Date&nbsp;&nbsp;</td>
          				<td align="left">&nbsp;</td>
          				<td align="right">Installment Amount</td>
          				<td align="center">Status/Status Date</td>
          				<td align="left">&nbsp;</td>
        			</tr>
        			
        			
         				
         				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2021-2022</td>
          					<td align="left">10191000</td>
          					<td align="left">$15,493.54</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=10191000&amp;pubBillType=SEC&amp;pubBillRollYear=2021" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2021&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$8,115.66</td>
		  					<td align="right" colspan="2">Cancelled Apr 6, 2022&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pplevenrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2022&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$7,377.88</td>
		  					<td align="right" colspan="2">Cancelled Apr 6, 2022&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2021-2022</td>
          					<td align="left">10191001</td>
          					<td align="left">$14,826.72</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=10191001&amp;pubBillType=SEC&amp;pubBillRollYear=2021" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">05/31/2022&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$7,408.36</td>
		  					<td align="right" colspan="2">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pploddrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">05/31/2022&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$7,418.36</td>
		  					<td align="right" colspan="2">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2020-2021</td>
          					<td align="left">10144200</td>
          					<td align="left">$7,948.62</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=10144200&amp;pubBillType=SEC&amp;pubBillRollYear=2020" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2020&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$3,969.31</td>
		  					<td align="right" colspan="2">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pplevenrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2021&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$3,979.31</td>
		  					<td align="right" colspan="2">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2019-2020</td>
          					<td align="left">10141700</td>
          					<td align="left">$6,460.56</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=10141700&amp;pubBillType=SEC&amp;pubBillRollYear=2019" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2019&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$3,225.28</td>
		  					<td align="right" colspan="2">Redeemed Feb 16, 2021&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pploddrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2020&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$3,235.28</td>
		  					<td align="right" colspan="2">Redeemed Feb 16, 2021&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2018-2019</td>
          					<td align="left">10135700</td>
          					<td align="left">$6,316.58</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=10135700&amp;pubBillType=SEC&amp;pubBillRollYear=2018" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2018&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$3,153.29</td>
		  					<td align="right" colspan="2">Redeemed Feb 16, 2021&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pplevenrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2019&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$3,163.29</td>
		  					<td align="right" colspan="2">Redeemed Feb 16, 2021&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2017-2018</td>
          					<td align="left">10082300</td>
          					<td align="left">$5,769.52</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=10082300&amp;pubBillType=SEC&amp;pubBillRollYear=2017" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2017&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,879.76</td>
		  					<td align="right" colspan="2">Redeemed Feb 16, 2021&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pploddrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2018&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,889.76</td>
		  					<td align="right" colspan="2">Redeemed Feb 16, 2021&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2016-2017</td>
          					<td align="left">10055000</td>
          					<td align="left">$5,264.02</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=10055000&amp;pubBillType=SEC&amp;pubBillRollYear=2016" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2016&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,757.34</td>
		  					<td align="right" colspan="2">Paid Jan 31, 2017&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pplevenrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2017&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,506.68</td>
		  					<td align="right" colspan="2">Paid Jan 31, 2017&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2015-2016</td>
          					<td align="left">10044900</td>
          					<td align="left">$5,475.46</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=10044900&amp;pubBillType=SEC&amp;pubBillRollYear=2015" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2015&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,732.73</td>
		  					<td align="right" colspan="2">Redeemed Feb 16, 2021&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pploddrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2016&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,742.73</td>
		  					<td align="right" colspan="2">Redeemed Feb 16, 2021&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2014-2015</td>
          					<td align="left">10042100</td>
          					<td align="left">$5,757.40</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=10042100&amp;pubBillType=SEC&amp;pubBillRollYear=2014" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2014&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,873.70</td>
		  					<td align="right" colspan="2">Redeemed Feb 16, 2021&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pplevenrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2015&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,883.70</td>
		  					<td align="right" colspan="2">Redeemed Feb 16, 2021&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2013-2014</td>
          					<td align="left">10028900</td>
          					<td align="left">$5,089.22</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=10028900&amp;pubBillType=SEC&amp;pubBillRollYear=2013" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2013&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,544.61</td>
		  					<td align="right" colspan="2">Paid Dec 10, 2013&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pploddrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2014&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,544.61</td>
		  					<td align="right" colspan="2">Paid Feb 18, 2014&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2012-2013</td>
          					<td align="left">10064200</td>
          					<td align="left">$5,162.17</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=10064200&amp;pubBillType=SEC&amp;pubBillRollYear=2012" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2012&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,703.99</td>
		  					<td align="right" colspan="2">Paid Apr 9, 2013&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pplevenrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2013&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,458.18</td>
		  					<td align="right" colspan="2">Paid Apr 9, 2013&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2011-2012</td>
          					<td align="left">10051800</td>
          					<td align="left">$5,476.50</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=10051800&amp;pubBillType=SEC&amp;pubBillRollYear=2011" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2011&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,868.64</td>
		  					<td align="right" colspan="2">Paid Apr 6, 2012&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pploddrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2012&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,607.86</td>
		  					<td align="right" colspan="2">Paid Apr 6, 2012&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2010-2011</td>
          					<td align="left">10035500</td>
          					<td align="left">$4,497.78</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=10035500&amp;pubBillType=SEC&amp;pubBillRollYear=2010" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2010&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,355.98</td>
		  					<td align="right" colspan="2">Paid Apr 10, 2011&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pplevenrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2011&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,141.80</td>
		  					<td align="right" colspan="2">Paid Apr 10, 2011&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2009-2010</td>
          					<td align="left">10010300</td>
          					<td align="left">$5,183.92</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=10010300&amp;pubBillType=SEC&amp;pubBillRollYear=2009" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2009&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,586.96</td>
		  					<td align="right" colspan="2">Paid Jun 30, 2010&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pploddrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2010&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,596.96</td>
		  					<td align="right" colspan="2">Paid Jun 30, 2010&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2008-2009</td>
          					<td align="left">09967400</td>
          					<td align="left">$4,935.38</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=09967400&amp;pubBillType=SEC&amp;pubBillRollYear=2008" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2008&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,462.69</td>
		  					<td align="right" colspan="2">Redeemed Apr 27, 2012&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pplevenrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2009&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,472.69</td>
		  					<td align="right" colspan="2">Redeemed Apr 27, 2012&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2007-2008</td>
          					<td align="left">09795600</td>
          					<td align="left">$4,829.06</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=09795600&amp;pubBillType=SEC&amp;pubBillRollYear=2007" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2007&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,409.53</td>
		  					<td align="right" colspan="2">Redeemed Apr 27, 2012&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pploddrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2008&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,419.53</td>
		  					<td align="right" colspan="2">Redeemed Apr 27, 2012&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2006-2007</td>
          					<td align="left">09666800</td>
          					<td align="left">$4,789.60</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=09666800&amp;pubBillType=SEC&amp;pubBillRollYear=2006" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2006&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,389.80</td>
		  					<td align="right" colspan="2">Redeemed Apr 27, 2012&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pplevenrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2007&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,399.80</td>
		  					<td align="right" colspan="2">Redeemed Apr 27, 2012&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2005-2006</td>
          					<td align="left">09576400</td>
          					<td align="left">$4,661.48</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=09576400&amp;pubBillType=SEC&amp;pubBillRollYear=2005" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pploddrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">12/10/2005&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,325.74</td>
		  					<td align="right" colspan="2">Redeemed Apr 27, 2012&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pploddrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">04/10/2006&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,335.74</td>
		  					<td align="right" colspan="2">Redeemed Apr 27, 2012&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
         				
         				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="left">Secured</td>
          					<td align="left">2004-2005</td>
          					<td align="left">09505700</td>
          					<td align="left">$4,004.54</td>
		  					<td align="left">&nbsp;
		  						
			  						<a href="/ptax_pub_app/PublicDownload.do?getStreamInfo=true&amp;pubBillId=09505700&amp;pubBillType=SEC&amp;pubBillRollYear=2004" tabindex="5" class="pplviewbill">View Bill</a>
    	    	  				
								&nbsp;&nbsp;&nbsp;&nbsp;
		  					</td>
          					<td align="left">&nbsp;</td>
        				</tr>
        				<tr class=pplevenrow>
       	  					<td align="left">&nbsp;</td>
          					<td align="center">1st Installment</td>
          					<td align="right">&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$1,997.27</td>
		  					<td align="right" colspan="2">Redeemed Apr 27, 2012&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
        				<tr class=pplevenrow>
	      					<td align="left">&nbsp;</td>
       	  					<td align="center">2nd Installment</td>
          					<td align="right">&nbsp;&nbsp;</td>
          					<td align="left">&nbsp;</td>
          					<td align="right">$2,007.27</td>
		  					<td align="right" colspan="2">Redeemed Apr 27, 2012&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
						</tr>
						
						
						
        			
	    		</table>
	      	</td>
      	</tr>
    </table>
    <br>&nbsp;
  


</form>
<script type="text/javascript" language="JavaScript">
  <!--
  var focusControl = document.forms["RealSearchForm"].elements["displayApn"];

  if (focusControl != null && focusControl.type != "hidden" && !focusControl.disabled && focusControl.style.display != "none") {
     focusControl.focus();
  }
  // -->
</script>




</BODY>
</html>

		<!-- </div> -->
		</div>
		</div>
</div>


	<table width="100%" border="0" cellspacing="0" cellpadding="0">
		
		<tr>
  			<td colspan="2">
<html>	
<body>
<div align="center" style="clear:both;">
<br>
<!-- ========================================================================================= -->
<!-- Alameda County Footer-->

	</div>
<footer id="newacgovfooter" style="width: 100%;text-align: center;float: left;font-size: 70%;font-weight: normal;font-style: normal;padding: 0px;margin: 10px;">
	<p style="padding:0px;margin:0px;display:inline;"><img style="vertical-align:middle;margin: 0px 5px;" src="/images/banner/footerlogo.png"
		 width="22" height="22" alt="Alameda County seal" />Alameda County &copy; 2020 &bull; All Rights Reserved &bull; <a
		 href="http://www.acgov.org/government/legal.htm">Legal / Disclaimers</a> &bull; <a href="http://www.acgov.org/government/accessibility.htm">Accessibility
		 </a> </p> </footer>

<!-- ========================================================================================= -->
</div>
</body>
</html></td>
		</tr>
	</table>

</BODY>
</html>
"""

current_tax_bill_re = re.compile("DISPLAY CURRENT BILL RESULTS[^$]*\\$([0-9,.]*).*DISPLAY PRIOR YEAR DELINQUENT TAX INFORMATION")
delinquent_tax_bill_re = re.compile("DISPLAY PRIOR YEAR DELINQUENT TAX INFORMATION[^$]*\\$([0-9,.]*).*DISPLAY TAX HISTORY")


def base_parse_tax_bill(regex, html_content):
    try:
        match = regex.search(html_content.replace("\n",""))
        if match is None:
            return None
        else:
            return_val = float(match.groups()[0].replace(",", ""))
            if return_val == 0:
                return None
            else:
                return return_val
    except:
        return None


def parse_current_tax_bill(html_content):
    return base_parse_tax_bill(current_tax_bill_re, html_content)


def parse_delinquent_tax_bill(html_content):
    return base_parse_tax_bill(delinquent_tax_bill_re, html_content)


current_udf = udf(parse_current_tax_bill, FloatType())
delinquent_udf = udf(parse_delinquent_tax_bill, FloatType())


def main():
    # Initialize a Spark session
    spark = SparkSession.builder.appName("TaxProcessing").getOrCreate()
    df = spark.read.format("org.apache.phoenix.spark").option("table", "tax_info") \
        .option("zkUrl", "namenode:2181").load()

    df = df.filter("LAST_DOWNLOADED is not NULL")\
        .withColumn("CURRENT_TAX_BILL", current_udf(df["html_contents"])) \
        .withColumn("DELINQUENT_TAX_BILL", delinquent_udf(df["html_contents"]))
    df = df.select("PARCEL_ID", "COUNTY", "CURRENT_TAX_BILL", "DELINQUENT_TAX_BILL")
    df.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "TAX_INFO_STATUS") \
        .option("zkUrl", "namenode:2181") \
        .save()


if __name__ == "__main__":
    main()
else:
    print("skipping")
