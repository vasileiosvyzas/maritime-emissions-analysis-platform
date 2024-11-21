import React from 'react';
import { useState } from 'react';
import { Layout } from 'lucide-react';
import { BarChart, LineChart, Bar, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import SignInModal from './SignInModal';
import icon from './icon_for_the_name_shipcarbontracker_blue (1).png';

const demoData = {
  emissions: [
    { month: 'Jan', cargo: 450, passenger: 380, tanker: 520 },
    { month: 'Feb', cargo: 470, passenger: 390, tanker: 510 },
    { month: 'Mar', cargo: 540, passenger: 400, tanker: 580 },
    { month: 'Apr', cargo: 580, passenger: 420, tanker: 620 },
    { month: 'May', cargo: 620, passenger: 450, tanker: 670 },
  ],
  trends: [
    { year: '2019', emissions: 1000 },
    { year: '2020', emissions: 950 },
    { year: '2021', emissions: 880 },
    { year: '2022', emissions: 820 },
    { year: '2023', emissions: 750 },
  ]
};

const LandingPage = () => {
  const [isMenuOpen, setIsMenuOpen] = useState(false);
  const [isSignInModalOpen, setIsSignInModalOpen] = useState(false);

  return (
    <div className="min-h-screen bg-gray-50">
        {/* SignIn Modal */}
      <SignInModal 
        isOpen={isSignInModalOpen} 
        onClose={() => setIsSignInModalOpen(false)} 
      />  
      {/* Navigation */}
      <nav className="bg-white shadow-lg">
        <div className="max-w-7xl mx-auto px-4">
          <div className="flex justify-between h-16">
            <div className="flex items-center">
              {/* <Layout className="h-8 w-8 text-blue-600" /> */}
              <img src={icon} alt="Platform Icon" className="h-8 w-8 mr-2" />
              <span className="ml-2 text-xl font-semibold">Maritime Emissions Analysis Platform</span>
            </div>
            
            <div className="hidden md:flex items-center space-x-4">
              <a href="#features" className="text-gray-700 hover:text-blue-600">Features</a>
              <a href="#documentation" className="text-gray-700 hover:text-blue-600">API Docs</a>
              <a href="#dashboard" className="text-gray-700 hover:text-blue-600">Dashboard</a>
              <button className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700" onClick={() => setIsSignInModalOpen(true)}>
                Sign In
              </button>
            </div>

            <button 
              className="md:hidden flex items-center"
              onClick={() => setIsMenuOpen(!isMenuOpen)}
            >
              <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
              </svg>
            </button>
          </div>
        </div>

        {/* Mobile menu */}
        {isMenuOpen && (
          <div className="md:hidden">
            <div className="px-2 pt-2 pb-3 space-y-1">
              <a href="#features" className="block px-3 py-2 text-gray-700 hover:bg-gray-100">Features</a>
              <a href="#documentation" className="block px-3 py-2 text-gray-700 hover:bg-gray-100">API Docs</a>
              <a href="#dashboard" className="block px-3 py-2 text-gray-700 hover:bg-gray-100">Dashboard</a>
              <button className="w-full text-left px-3 py-2 text-gray-700 hover:bg-gray-100" onClick={() => setIsSignInModalOpen(true)}>
                Sign In
              </button>
            </div>
          </div>
        )}
      </nav>

      {/* Hero Section */}
      <div className="bg-blue-600 text-white py-16">
        <div className="max-w-7xl mx-auto px-4">
          <div className="text-center">
            <h1 className="text-4xl font-bold mb-4">
              Maritime Emissions Analysis Platform
            </h1>
            <p className="text-xl mb-8">
              Comprehensive insights for maritime emissions monitoring and optimization
            </p>
            <button className="bg-white text-blue-600 px-6 py-3 rounded-lg font-semibold hover:bg-gray-100">
              Request API Access
            </button>
          </div>
        </div>
      </div>

      {/* Features Section */}
      <div id="features" className="py-16">
        <div className="max-w-7xl mx-auto px-4">
          <h2 className="text-3xl font-bold text-center mb-12">Key Features</h2>
          <div className="grid md:grid-cols-3 gap-8">
            <div className="bg-white p-6 rounded-lg shadow">
              <h3 className="text-xl font-semibold mb-4">RESTful API</h3>
              <p className="text-gray-600">
                Access maritime emissions data through our RESTful API with comprehensive documentation.
              </p>
            </div>
            <div className="bg-white p-6 rounded-lg shadow">
              <h3 className="text-xl font-semibold mb-4">Interactive Dashboard</h3>
              <p className="text-gray-600">
                Visualize and analyze emissions data with customizable charts and filters.
              </p>
            </div>
            <div className="bg-gray-200 p-6 rounded-lg shadow opacity-80 pointer-events-none">
              <h3 className="text-xl font-semibold mb-4 text-gray-400">Detailed Reports (Coming soon)</h3>
              <p className="text-gray-400">
                Generate and download comprehensive PDF reports for deeper analysis.
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Charts Section */}
      <div id="dashboard" className="py-16 bg-gray-100">
        <div className="max-w-7xl mx-auto px-4">
          <h2 className="text-3xl font-bold text-center mb-12">Data Insights</h2>
          <div className="grid md:grid-cols-2 gap-8">
            <div className="bg-white p-6 rounded-lg shadow">
              <h3 className="text-xl font-semibold mb-4">Emissions by Ship Type</h3>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={demoData.emissions}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="month" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="cargo" fill="#3B82F6" />
                    <Bar dataKey="passenger" fill="#10B981" />
                    <Bar dataKey="tanker" fill="#6366F1" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
            <div className="bg-white p-6 rounded-lg shadow">
              <h3 className="text-xl font-semibold mb-4">Historical Trends</h3>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={demoData.trends}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="year" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Line type="monotone" dataKey="emissions" stroke="#3B82F6" />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* CTA Section */}
      <div className="bg-blue-600 text-white py-16">
        <div className="max-w-7xl mx-auto px-4 text-center">
          <h2 className="text-3xl font-bold mb-4">Ready to Get Started?</h2>
          <p className="text-xl mb-8">
            Sign up now to access our API and start analyzing maritime emissions data.
          </p>
          <button className="bg-white text-blue-600 px-6 py-3 rounded-lg font-semibold hover:bg-gray-100">
            Create Account
          </button>
        </div>
      </div>

      {/* Footer */}
      <footer className="bg-gray-800 text-white py-8">
        <div className="max-w-7xl mx-auto px-4">
          <div className="grid md:grid-cols-3 gap-8">
            <div>
              <h3 className="text-lg font-semibold mb-4">Maritime Emissions Analysis Platform</h3>
              <p className="text-gray-400">
                Helping the maritime industry monitor and reduce emissions.
              </p>
            </div>
            <div>
              <h3 className="text-lg font-semibold mb-4">Quick Links</h3>
              <ul className="space-y-2">
                <li><a href="#features" className="text-gray-400 hover:text-white">Features</a></li>
                <li><a href="#documentation" className="text-gray-400 hover:text-white">API Documentation</a></li>
                <li><a href="#dashboard" className="text-gray-400 hover:text-white">Dashboard</a></li>
              </ul>
            </div>
            <div>
              <h3 className="text-lg font-semibold mb-4">Contact</h3>
              <p className="text-gray-400">
                Get in touch for support or feedback
              </p>
            </div>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default LandingPage;