import { Routes, Route, Navigate, useParams, useSearchParams } from "react-router";
import { useStudies } from "./api/studies";
import { TraitsLandingPage } from "./pages/traits-landing";
import { TraitDetailPage } from "./pages/trait-detail";
import { GenesPage } from "./pages/genes";
import { GeneDetailPage } from "./pages/gene-detail";
import { SourcesPage } from "./pages/sources";
import { QueryPage } from "./pages/query";
import { SettingsPage } from "./pages/settings";
import { Loading } from "./components/loading";

/** Redirects /studies/:studyId → /traits/:trait, preserving query params. */
function StudyRedirect() {
  const { "*": studyId } = useParams();
  const [searchParams] = useSearchParams();
  const { data: studies, isLoading } = useStudies();

  if (isLoading) return <Loading />;

  const study = studies?.find((s) => s.study_id === studyId);
  if (study) {
    const params = searchParams.toString();
    const target = `/traits/${encodeURIComponent(study.trait)}${params ? `?${params}` : ""}`;
    return <Navigate to={target} replace />;
  }

  // Unknown study — fall back to landing
  return <Navigate to="/" replace />;
}

export function AppRoutes() {
  return (
    <Routes>
      <Route index element={<TraitsLandingPage />} />
      <Route path="traits/:trait" element={<TraitDetailPage />} />
      <Route path="genes" element={<GenesPage />} />
      <Route path="genes/:gene" element={<GeneDetailPage />} />
      <Route path="sources" element={<SourcesPage />} />
      <Route path="query" element={<QueryPage />} />
      <Route path="settings" element={<SettingsPage />} />
      {/* Legacy redirects */}
      <Route path="studies" element={<Navigate to="/" replace />} />
      <Route path="studies/*" element={<StudyRedirect />} />
    </Routes>
  );
}
