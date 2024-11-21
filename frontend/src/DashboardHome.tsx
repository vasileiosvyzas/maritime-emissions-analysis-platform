import React from 'react';

const DashboardHome = () => {
  const handleRequestApiAccess = () => {
    // Navigate to API request page or open a modal for requesting access
    window.location.href = '/request-api';
  };

  const handleReadDocumentation = () => {
    // Navigate to API documentation page
    window.location.href = 'https://maritime-emissions.readme.io/reference/get_';
  };

  const handleOpenDashboard = () => {
    // Redirect to an external or internal dashboard URL
    window.location.href = 'http://localhost:8088/superset/dashboard/p/XRGL1P2kjmJ/';
  };

  return (
    <div className="min-h-screen bg-gray-100 flex items-center justify-center py-12">
      <div className="max-w-7xl mx-auto px-4">
        <h1 className="text-3xl font-bold text-center mb-8">Welcome to the Maritime Emissions Analysis Platform</h1>
        
        <div className="grid md:grid-cols-3 gap-8">
          {/* Request API Access Card */}
          <div className="bg-white p-6 rounded-lg shadow-lg text-center">
            <h2 className="text-xl font-semibold mb-4">Request API Access</h2>
            <p className="text-gray-600 mb-6">
              Get access to our API to retrieve and analyze maritime emissions data.
            </p>
            <button
              onClick={handleRequestApiAccess}
              className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition duration-300"
            >
              Request Access
            </button>
          </div>

          {/* Read Documentation Card */}
          <div className="bg-white p-6 rounded-lg shadow-lg text-center">
            <h2 className="text-xl font-semibold mb-4">Read API Documentation</h2>
            <p className="text-gray-600 mb-6">
              Explore our detailed documentation to get started with API integration.
            </p>
            <button
              onClick={handleReadDocumentation}
              className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition duration-300"
            >
              Read Docs
            </button>
          </div>

          {/* Open Dashboard Card */}
          <div className="bg-white p-6 rounded-lg shadow-lg text-center">
            <h2 className="text-xl font-semibold mb-4">Open Dashboard</h2>
            <p className="text-gray-600 mb-6">
              View real-time data and insights on maritime emissions in the dashboard.
            </p>
            <button
              onClick={handleOpenDashboard}
              className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition duration-300"
            >
              Go to Dashboard
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DashboardHome;