import React from 'react';
import LandingPage from './LandingPage';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import DashboardHome from './DashboardHome';
import APIAccessRequest from './request_form';

function App() {
  const userEmail = "user@example.com";
  return (
    <Router>
      <Routes>
        <Route path="/" element={<LandingPage />} />  {/* Sign-In page */}
        <Route path="/dashboard-home" element={<DashboardHome />} />  {/* Post-login page */}
        <Route path="/request_form" element={<APIAccessRequest userEmail={userEmail} />} />  {/* API request form page */}
      </Routes>
    </Router>
    // <div>
    //   <LandingPage />
    // </div>
  );
}

export default App;