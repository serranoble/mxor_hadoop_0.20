package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;
import java.text.DecimalFormat;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public final class machines_005ftxt_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {


  public void generateTaskTrackerTable(JspWriter out,
                                       String type,
                                       JobTracker tracker) throws IOException {
    Collection<TaskTrackerStatus> c;
    if (("BLACKLISTED").equalsIgnoreCase(type)) {
      c = tracker.blacklistedTaskTrackers();
    } else if (("ACTIVE").equalsIgnoreCase(type)) {
      c = tracker.activeTaskTrackers();
    } else {
      c = tracker.taskTrackers();
    }
    for (TaskTrackerStatus tt : c) {
      out.print(tt.getHost() + "\n");
    }
  }
  public void generateTableForExcludedNodes(JspWriter out, JobTracker tracker) 
  throws IOException {
    // excluded nodes
    for (String tt : tracker.getExcludedNodes()) {
      out.print(tt + "\n");
    }
  }

  private static final JspFactory _jspxFactory = JspFactory.getDefaultFactory();

  private static java.util.Vector _jspx_dependants;

  private org.apache.jasper.runtime.ResourceInjector _jspx_resourceInjector;

  public Object getDependants() {
    return _jspx_dependants;
  }

  public void _jspService(HttpServletRequest request, HttpServletResponse response)
        throws java.io.IOException, ServletException {

    PageContext pageContext = null;
    HttpSession session = null;
    ServletContext application = null;
    ServletConfig config = null;
    JspWriter out = null;
    Object page = this;
    JspWriter _jspx_out = null;
    PageContext _jspx_page_context = null;

    try {
      response.setContentType("text/plain; charset=UTF-8");
      pageContext = _jspxFactory.getPageContext(this, request, response,
      			null, true, 8192, true);
      _jspx_page_context = pageContext;
      application = pageContext.getServletContext();
      config = pageContext.getServletConfig();
      session = pageContext.getSession();
      out = pageContext.getOut();
      _jspx_out = out;
      _jspx_resourceInjector = (org.apache.jasper.runtime.ResourceInjector) application.getAttribute("com.sun.appserv.jsp.resource.injector");

      out.write('\n');

  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String type = request.getParameter("type");

      out.write('\n');
      out.write('\n');

  if (("EXCLUDED").equalsIgnoreCase(type)) {
    generateTableForExcludedNodes(out, tracker);
  } else {
    generateTaskTrackerTable(out, type, tracker);
  }

      out.write('\n');
    } catch (Throwable t) {
      if (!(t instanceof SkipPageException)){
        out = _jspx_out;
        if (out != null && out.getBufferSize() != 0)
          out.clearBuffer();
        if (_jspx_page_context != null) _jspx_page_context.handlePageException(t);
      }
    } finally {
      _jspxFactory.releasePageContext(_jspx_page_context);
    }
  }
}
