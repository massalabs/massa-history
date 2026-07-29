import { Route, Routes, useLocation } from "react-router-dom";
import { AppStateProvider } from "./AppState";
import { Layout } from "./components/Layout";
import { ErrorBoundary } from "./components/ErrorBoundary";
import { Home } from "./pages/Home";
import { SlotDetail } from "./pages/SlotDetail";
import { BlockDetail } from "./pages/BlockDetail";
import { OperationDetail } from "./pages/OperationDetail";
import { AddressDetail } from "./pages/AddressDetail";
import { Settings } from "./pages/Settings";
import { Search } from "./pages/Search";
import { NotFoundPage } from "./pages/NotFoundPage";
import { Blocks } from "./pages/Blocks";
import { Operations } from "./pages/Operations";
import { EndorsementDetail } from "./pages/EndorsementDetail";
import { DenunciationDetail } from "./pages/DenunciationDetail";
import { Denunciations } from "./pages/Denunciations";
import { Charts } from "./pages/Charts";
import { ApiDocs } from "./pages/ApiDocs";

function RoutedContent() {
  const location = useLocation();
  return (
    // key={pathname} resets the boundary on route change so users can navigate
    // away from a crashed page.
    <ErrorBoundary key={location.pathname}>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/blocks" element={<Blocks />} />
        <Route path="/operations" element={<Operations />} />
        <Route path="/denunciations" element={<Denunciations />} />
        <Route path="/charts" element={<Charts />} />
        <Route path="/api" element={<ApiDocs />} />
        <Route path="/slot/:period/:thread" element={<SlotDetail />} />
        <Route path="/block/:id" element={<BlockDetail />} />
        <Route path="/op/:id" element={<OperationDetail />} />
        <Route path="/endorsement/:id" element={<EndorsementDetail />} />
        <Route path="/denunciation/:hash" element={<DenunciationDetail />} />
        <Route path="/address/:addr" element={<AddressDetail />} />
        <Route path="/search" element={<Search />} />
        <Route path="/settings" element={<Settings />} />
        <Route path="*" element={<NotFoundPage />} />
      </Routes>
    </ErrorBoundary>
  );
}

export default function App() {
  return (
    <AppStateProvider>
      <Layout>
        <RoutedContent />
      </Layout>
    </AppStateProvider>
  );
}
