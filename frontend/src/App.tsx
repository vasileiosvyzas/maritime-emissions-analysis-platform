import React from 'react';
import LandingPage from './LandingPage';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import SignInModal from './SignInModal';
import DashboardHome from './DashboardHome';

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<LandingPage />} />  {/* Sign-In page */}
        <Route path="/dashboard-home" element={<DashboardHome />} />  {/* Post-login page */}
      </Routes>
    </Router>
    // <div>
    //   <LandingPage />
    // </div>
  );
}

export default App;